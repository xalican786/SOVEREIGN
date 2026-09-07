// src/dashboard.js — SOVEREIGN 10-tab dashboard
// Fixed: injects wss/ws auto-detect into sovereign.html at serve time
// Fixed: fullState never throws, SAB-safe, amplifier lazy-loaded

import { createRequire }           from 'module'
import { createServer }            from 'http'
import { existsSync, readFileSync } from 'fs'
import { fileURLToPath }           from 'url'
import path                        from 'path'

const __dir = path.dirname(fileURLToPath(import.meta.url))
const _req  = createRequire(import.meta.url)
const express             = _req(path.join(__dir, '../node_modules/express'))
const { WebSocketServer } = _req(path.join(__dir, '../node_modules/ws'))

import {
  H, PORT, SYSTEM, VERSION, EXECUTOR, TREASURY,
  CONTRACT, CHAINS, PROPELLER, FLASH,
  TOTAL_FLASH_CAPACITY,
} from './config.js'
import { activatePropeller, getPropellerStats, getVelocity } from './propeller.js'
import { getBundleStats }                                     from './bundle.js'
import { reconcile }                                          from './treasury.js'
import { send as mpSend, calcFee, networks }                  from './adapters/modempay.js'

// ── AMPLIFIER — lazy to survive missing/broken module ────────────────────────
let _layerBreakdown = () => []
let _updateLayer    = () => false
let _blendedRate    = () => 0

import('./amplifier.js')
  .then(m => {
    if (typeof m.layerBreakdown === 'function') _layerBreakdown = m.layerBreakdown
    if (typeof m.updateLayer    === 'function') _updateLayer    = m.updateLayer
    if (typeof m.blendedRate    === 'function') _blendedRate    = m.blendedRate
    console.log('[DASHBOARD] Amplifier module loaded')
  })
  .catch(e => console.log('[DASHBOARD] Amplifier load failed:', e.message?.slice(0, 60)))

// ── SAB ───────────────────────────────────────────────────────────────────────
let SAB_REF      = null
const WS_CLIENTS = new Set()
const hot        = () => SAB_REF ? new Float64Array(SAB_REF) : null

// ── FORMAT ────────────────────────────────────────────────────────────────────
function fmt(n) {
  if (!n || isNaN(n)) return '$0.00'
  if (n >= 1e12) return `$${(n/1e12).toFixed(3)}T`
  if (n >= 1e9)  return `$${(n/1e9).toFixed(3)}B`
  if (n >= 1e6)  return `$${(n/1e6).toFixed(3)}M`
  if (n >= 1e3)  return `$${(n/1e3).toFixed(2)}K`
  return `$${n.toFixed(2)}`
}

// ── FULL STATE ────────────────────────────────────────────────────────────────
function fullState() {
  try {
    const H2  = hot()
    const base = {
      type:          'state',
      ts:            Date.now(),
      system:        SYSTEM,
      version:       VERSION,
      executor:      EXECUTOR,
      treasury:      TREASURY,
      contractAddrs: CONTRACT,
      flashCap:      TOTAL_FLASH_CAPACITY,
      flashPerChain: FLASH,
      wsClients:     WS_CLIENTS.size,
    }

    if (!H2) {
      return {
        ...base, ready: false,
        revToday: 0, revTotal: 0, netToday: 0,
        swapsToday: 0, naturalToday: 0, execToday: 0,
        successToday: 0, failToday: 0, execSpeed: 0,
        peakSwap: 0, avgSwap: 0, flashDeployed: 0,
        gasPrice: 0, gasOK: false,
        propeller: 'P1', dailyTarget: 0, progress: 0, velocity: 0,
        propellerStats: [],
        vaultBalance: 0, recyclerBal: 0, firstRev: false,
        contracts: 0, deployment: false, uptime: 0, chainCount: 0,
        chainStates: {}, ampLayers: [], blendedRate: 0, bundleStats: {},
      }
    }

    return {
      ...base, ready: true,
      // Revenue
      revToday:  H2[H.REV_TODAY]  || 0,
      revTotal:  H2[H.REV_TOTAL]  || 0,
      netToday:  H2[H.NET_TODAY]  || 0,
      // Execution
      swapsToday:   H2[H.SWAPS_TODAY]   | 0,
      swapsTotal:   H2[H.SWAPS_TOTAL]   | 0,
      naturalToday: H2[H.NATURAL_TODAY] | 0,
      execToday:    H2[H.EXEC_TODAY]    | 0,
      successToday: H2[H.SUCCESS_TODAY] | 0,
      failToday:    H2[H.FAIL_TODAY]    | 0,
      execSpeed:    H2[H.EXEC_SPEED_MS] || 0,
      peakSwap:     H2[H.PEAK_SWAP]     || 0,
      avgSwap:      H2[H.AVG_SWAP]      || 0,
      // Flash
      flashDeployed: H2[H.FLASH_DEPLOYED] || 0,
      // Gas
      gasPrice: H2[H.GAS_PRICE] || 0,
      gasOK:    H2[H.GAS_OK]    === 1,
      // Propeller
      propeller:      'P' + (H2[H.PROPELLER] | 0 || 1),
      dailyTarget:    H2[H.DAILY_TARGET] || (PROPELLER.P1?.target || 0),
      progress:       H2[H.PROGRESS]    || 0,
      velocity:       (() => { try { return getVelocity(H2)    } catch { return 0  } })(),
      propellerStats: (() => { try { return getPropellerStats() } catch { return [] } })(),
      // Vault
      vaultBalance: H2[H.VAULT_BALANCE] || 0,
      recyclerBal:  H2[H.RECYCLER_BAL]  || 0,
      firstRev:     H2[H.FIRST_REV]     === 1,
      // System
      contracts:  H2[H.CONTRACTS]  | 0,
      deployment: H2[H.DEPLOYMENT] === 1,
      uptime:     H2[H.UPTIME]     | 0,
      chainCount: H2[H.CHAIN_COUNT]| 0,
      // Chains
      chainStates: Object.fromEntries(
        CHAINS.map(c => [c.name, H2[H['C_' + c.name.toUpperCase()]] === 1])
      ),
      // Amplifier
      ampLayers:   (() => { try { return _layerBreakdown() } catch { return [] } })(),
      blendedRate: (() => { try { return _blendedRate()    } catch { return 0  } })(),
      // Bundles
      bundleStats: (() => { try { return getBundleStats()  } catch { return {} } })(),
    }
  } catch (e) {
    console.log('[DASHBOARD] fullState error:', e.message?.slice(0, 80))
    return { type: 'state', ts: Date.now(), ready: false, wsClients: WS_CLIENTS.size }
  }
}

// ── BROADCAST ─────────────────────────────────────────────────────────────────
function broadcast(data) {
  const p = JSON.stringify(data)
  for (const ws of WS_CLIENTS) {
    if (ws.readyState === 1) {
      try { ws.send(p) } catch { WS_CLIENTS.delete(ws) }
    }
  }
}

setInterval(() => { if (WS_CLIENTS.size > 0) broadcast(fullState()) }, 500)

// ── WS PROTOCOL FIX — injected into HTML at serve time ───────────────────────
// Railway runs HTTPS. The HTML hardcodes ws:// which browsers block (mixed content).
// We inject a tiny shim before </head> that patches WebSocket to use wss:// on HTTPS.
const WS_SHIM = `<script>
;(function(){
  var _WS = window.WebSocket
  window.WebSocket = function(url, proto) {
    if (location.protocol === 'https:' && url && url.indexOf('ws://') === 0)
      url = url.replace('ws://', 'wss://')
    return proto ? new _WS(url, proto) : new _WS(url)
  }
  Object.assign(window.WebSocket, _WS)
})()
</script>`

// ── EXPRESS ───────────────────────────────────────────────────────────────────
const app = express()
const srv = createServer(app)
const wss = new WebSocketServer({ server: srv, perMessageDeflate: false })

app.use(express.json({ limit: '1mb' }))

// Serve static assets (css, etc) from dashboard/
app.use(express.static(path.join(__dir, '../dashboard')))

// Serve HTML with WS shim injected
app.get('/', (req, res) => {
  const p = path.join(__dir, '../dashboard/sovereign.html')
  if (!existsSync(p)) return res.status(404).send('sovereign.html missing')
  try {
    let html = readFileSync(p, 'utf8')
    // Inject shim just before </head> so it runs before the page script
    html = html.replace('</head>', WS_SHIM + '\n</head>')
    res.setHeader('Content-Type', 'text/html; charset=utf-8')
    res.send(html)
  } catch (e) {
    res.status(500).send('Failed to read sovereign.html: ' + e.message)
  }
})

// ── API ───────────────────────────────────────────────────────────────────────
app.get('/ping', (req, res) => {
  const H2 = hot()
  res.json({
    ok: true, system: SYSTEM, version: VERSION,
    uptime:   H2?.[H.UPTIME]      | 0,
    deployed: H2?.[H.DEPLOYMENT]  === 1,
    chains:   H2?.[H.CHAIN_COUNT] | 0,
    sabReady: !!SAB_REF,
  })
})

app.get('/api/state',     (_, res) => res.json(fullState()))
app.get('/api/layers',    (_, res) => res.json({ layers: _layerBreakdown(), blended: _blendedRate() }))
app.get('/api/flash',     (_, res) => res.json({ perChain: FLASH, total: TOTAL_FLASH_CAPACITY }))
app.get('/api/propeller', (_, res) => { try { res.json(getPropellerStats()) } catch { res.json([]) } })

app.post('/api/propeller', (req, res) => {
  const { level } = req.body
  const H2 = hot(); if (!H2) return res.status(503).json({ error: 'SAB not ready' })
  try { res.json({ ok: activatePropeller(level, H2), level, target: PROPELLER[level]?.target }) }
  catch (e) { res.status(500).json({ error: e.message }) }
})

app.post('/api/propeller/target', (req, res) => {
  const { target } = req.body
  const H2 = hot(); if (!H2) return res.status(503).json({ error: 'SAB not ready' })
  if (!target || target <= 0) return res.status(400).json({ error: 'invalid target' })
  H2[H.DAILY_TARGET] = parseFloat(target)
  res.json({ ok: true, target })
})

app.post('/api/amplifier/layer', (req, res) => {
  const { id, rate } = req.body
  try { res.json({ ok: _updateLayer(parseInt(id), parseFloat(rate)), layers: _layerBreakdown() }) }
  catch (e) { res.status(500).json({ error: e.message }) }
})

app.post('/api/amplifier/toggle', (req, res) => {
  const { id, active } = req.body
  try { res.json({ ok: _updateLayer(parseInt(id), undefined, active), layers: _layerBreakdown() }) }
  catch (e) { res.status(500).json({ error: e.message }) }
})

app.post('/api/executor/pause', (req, res) => {
  const H2 = hot(); if (!H2) return res.status(503).json({ error: 'SAB not ready' })
  H2[H.GAS_OK] = 0; res.json({ ok: true, status: 'paused' })
})

app.post('/api/executor/resume', (req, res) => {
  const H2 = hot(); if (!H2) return res.status(503).json({ error: 'SAB not ready' })
  H2[H.GAS_OK] = 1; res.json({ ok: true, status: 'resumed' })
})

app.post('/api/executor/reset', (req, res) => {
  const H2 = hot(); if (!H2) return res.status(503).json({ error: 'SAB not ready' })
  H2[H.SWAPS_TODAY] = H2[H.SUCCESS_TODAY] = H2[H.FAIL_TODAY] =
  H2[H.EXEC_TODAY]  = H2[H.REV_TODAY]     = H2[H.NET_TODAY]  = 0
  res.json({ ok: true })
})

app.post('/api/executor/gascap', (req, res) => {
  const { gwei } = req.body
  const H2 = hot(); if (!H2) return res.status(503).json({ error: 'SAB not ready' })
  if (!gwei || gwei <= 0) return res.status(400).json({ error: 'invalid gwei' })
  H2[H.GAS_CAP] = parseFloat(gwei); res.json({ ok: true, gwei })
})

app.post('/api/executor/concurrent', (req, res) => {
  const { max } = req.body
  const H2 = hot(); if (!H2) return res.status(503).json({ error: 'SAB not ready' })
  if (!max || max <= 0) return res.status(400).json({ error: 'invalid max' })
  H2[H.MAX_CONCURRENT] = parseInt(max); res.json({ ok: true, max })
})

app.post('/api/treasury/reconcile', async (req, res) => {
  const H2 = hot(); if (!H2) return res.status(503).json({ error: 'SAB not ready' })
  try { await reconcile(H2); res.json({ ok: true, balance: H2[H.VAULT_BALANCE] }) }
  catch (e) { res.status(500).json({ error: e.message }) }
})

app.post('/api/ftw/quote', (req, res) => {
  const { amount, network } = req.body
  if (!amount || amount <= 0) return res.status(400).json({ error: 'amount required' })
  try { res.json({ ...calcFee(parseFloat(amount), network || 'wave'), ts: Date.now() }) }
  catch (e) { res.status(500).json({ error: e.message }) }
})

app.post('/api/ftw/withdraw', async (req, res) => {
  const { amount, type, phone, accountNumber, accountName, swiftCode, network, address } = req.body
  if (!amount || amount <= 0) return res.status(400).json({ error: 'amount required' })
  const key = process.env.MODEMPAY_SECRET_KEY || ''
  if (!key) return res.status(400).json({ error: 'MODEMPAY_SECRET_KEY not set in Railway env vars' })
  try {
    const result = await mpSend(key, {
      type, amount: parseFloat(amount),
      phone, accountNumber, accountName, swiftCode, network, address,
    })
    broadcast({ type: 'ftw', amount, network: result.network })
    res.json({ ok: true, ...result })
  } catch (e) { res.status(500).json({ error: e.message?.slice(0, 120) }) }
})

app.get('/api/ftw/networks', (_, res) => {
  try { res.json(networks()) } catch { res.json([]) }
})

// ── WEBSOCKET ─────────────────────────────────────────────────────────────────
wss.on('connection', ws => {
  WS_CLIENTS.add(ws)
  try { ws.send(JSON.stringify(fullState())) } catch {}
  ws.on('close', () => WS_CLIENTS.delete(ws))
  ws.on('error', () => WS_CLIENTS.delete(ws))
})

// ── EXPORT ────────────────────────────────────────────────────────────────────
export function startDashboard(SAB) {
  SAB_REF = SAB
  srv.listen(PORT, () => {
    console.log(`[DASHBOARD] SOVEREIGN :${PORT} | 10 tabs | /ping`)
  })
}
