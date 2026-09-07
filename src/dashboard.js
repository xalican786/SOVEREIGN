// src/dashboard.js — SOVEREIGN 10-tab dashboard
// Tabs: Overview, Executor, Amplifier, Chains, Flash, Propeller, Vault, Contracts, Bundles, FTW
// WebSocket broadcast every 500ms
// ModemPay FTW withdrawal endpoint

import { createRequire }  from 'module'
import { createServer }   from 'http'
import { existsSync }     from 'fs'
import { fileURLToPath }  from 'url'
import path               from 'path'

const __dir = path.dirname(fileURLToPath(import.meta.url))
const _req  = createRequire(import.meta.url)
const express             = _req(path.join(__dir, '../node_modules/express'))
const { WebSocketServer } = _req(path.join(__dir, '../node_modules/ws'))

import {
  H, PORT, SYSTEM, VERSION, EXECUTOR, TREASURY,
  CONTRACT, CHAINS, PROPELLER, FLASH,
  TOTAL_FLASH_CAPACITY,
} from './config.js'
import { activatePropeller, getPropellerStats, getProgress, getVelocity } from './propeller.js'
import { getBundleStats }                             from './bundle.js'
import { startTreasury, reconcile }                   from './treasury.js'
import { send as mpSend, calcFee, networks }          from './adapters/modempay.js'

// ── AMPLIFIER — lazy-loaded to avoid circular import / missing export crashes ──
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

// ── SAB reference — set by startDashboard(SAB) ────────────────────────────────
let SAB_REF      = null
const WS_CLIENTS = new Set()

// Returns a live Float64Array view, or null if SAB not yet set
const hot = () => SAB_REF ? new Float64Array(SAB_REF) : null

// ── FORMAT ────────────────────────────────────────────────────────────────────
function fmt(n) {
  if (!n || isNaN(n)) return '$0.00'
  if (n >= 1e12) return `$${(n/1e12).toFixed(3)}T`
  if (n >= 1e9)  return `$${(n/1e9).toFixed(3)}B`
  if (n >= 1e6)  return `$${(n/1e6).toFixed(3)}M`
  if (n >= 1e3)  return `$${(n/1e3).toFixed(2)}K`
  return `$${n.toFixed(2)}`
}

// ── FULL STATE — safe, never throws ──────────────────────────────────────────
function fullState() {
  try {
    const H2 = hot()

    // SAB not yet wired — send a loading state the client can display
    if (!H2) {
      return {
        type: 'state', ts: Date.now(), booting: false, loading: true,
        system: SYSTEM, version: VERSION, executor: EXECUTOR,
        revToday: 0, revTotal: 0, netToday: 0,
        swapsToday: 0, naturalToday: 0, execToday: 0,
        successToday: 0, failToday: 0,
        gasPrice: 0, gasOK: false,
        propeller: 'P1', dailyTarget: 0, progress: 0, velocity: 0,
        vaultBalance: 0, recyclerBal: 0, firstRev: false,
        contracts: 0, deployment: false, uptime: 0,
        chainCount: 0, chainStates: {},
        ampLayers: [], blendedRate: 0,
        bundleStats: {}, contractAddrs: CONTRACT,
        flashCap: TOTAL_FLASH_CAPACITY, flashDeployed: 0,
        wsClients: WS_CLIENTS.size,
      }
    }

    return {
      type: 'state', ts: Date.now(), booting: false, loading: false,
      system: SYSTEM, version: VERSION,

      // Revenue
      revToday:  H2[H.REV_TODAY]  || 0,
      revTotal:  H2[H.REV_TOTAL]  || 0,
      netToday:  H2[H.NET_TODAY]  || 0,
      revFmt:    fmt(H2[H.REV_TODAY] || 0),

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
      flashCap:      TOTAL_FLASH_CAPACITY,
      flashPerChain: FLASH,

      // Gas
      gasPrice: H2[H.GAS_PRICE] || 0,
      gasOK:    H2[H.GAS_OK] === 1,

      // Propeller
      propeller:      'P' + (H2[H.PROPELLER] | 0 || 1),
      propellerNum:   H2[H.PROPELLER] | 0,
      dailyTarget:    H2[H.DAILY_TARGET] || (PROPELLER.P1?.target || 0),
      progress:       H2[H.PROGRESS]    || 0,
      velocity:       (() => { try { return getVelocity(H2) } catch { return 0 } })(),
      propellerStats: (() => { try { return getPropellerStats() } catch { return [] } })(),

      // Vault
      vaultBalance: H2[H.VAULT_BALANCE]  || 0,
      recyclerBal:  H2[H.RECYCLER_BAL]   || 0,
      firstRev:     H2[H.FIRST_REV]      === 1,

      // System
      contracts:  H2[H.CONTRACTS]   | 0,
      deployment: H2[H.DEPLOYMENT]  === 1,
      uptime:     H2[H.UPTIME]      | 0,
      mb:         H2[H.MB]          | 0,
      chainCount: H2[H.CHAIN_COUNT] | 0,
      executor:   EXECUTOR,
      treasury:   TREASURY,
      contractAddrs: CONTRACT,

      // Chains
      chainStates: Object.fromEntries(
        CHAINS.map(c => [c.name, H2[H['C_' + c.name.toUpperCase()]] === 1])
      ),

      // Amplifier — lazy-loaded, safe fallbacks
      ampLayers:   (() => { try { return _layerBreakdown() } catch { return [] } })(),
      blendedRate: (() => { try { return _blendedRate()    } catch { return 0  } })(),

      // Bundles
      bundleStats: (() => { try { return getBundleStats() } catch { return {} } })(),

      wsClients: WS_CLIENTS.size,
    }
  } catch (e) {
    // fullState must never throw — log and return a safe skeleton
    console.log('[DASHBOARD] fullState error:', e.message?.slice(0, 80))
    return {
      type: 'state', ts: Date.now(), booting: false, loading: true,
      error: e.message?.slice(0, 80),
      wsClients: WS_CLIENTS.size,
    }
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

// Always broadcast — even before SAB is wired, clients get a loading state
setInterval(() => {
  if (WS_CLIENTS.size > 0) broadcast(fullState())
}, 500)

// ── EXPRESS + WS ──────────────────────────────────────────────────────────────
const app = express()
const srv = createServer(app)
const wss = new WebSocketServer({ server: srv, perMessageDeflate: false })

app.use(express.json({ limit: '1mb' }))
app.use(express.static(path.join(__dir, '../dashboard')))

app.get('/', (_, res) => {
  const p = path.join(__dir, '../dashboard/sovereign.html')
  existsSync(p) ? res.sendFile(p) : res.status(404).send('sovereign.html missing')
})

app.get('/ping', (_, res) => {
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
app.get('/api/propeller', (_, res) => {
  try { res.json(getPropellerStats()) } catch (e) { res.json([]) }
})

// Set propeller level
app.post('/api/propeller', (req, res) => {
  const { level } = req.body
  const H2 = hot()
  if (!H2) return res.status(503).json({ error: 'SAB not ready' })
  try {
    const ok = activatePropeller(level, H2)
    res.json({ ok, level, target: PROPELLER[level]?.target })
  } catch (e) {
    res.status(500).json({ error: e.message })
  }
})

// Custom propeller target
app.post('/api/propeller/target', (req, res) => {
  const { target } = req.body
  const H2 = hot()
  if (!H2) return res.status(503).json({ error: 'SAB not ready' })
  if (!target || target <= 0) return res.status(400).json({ error: 'invalid target' })
  H2[H.DAILY_TARGET] = parseFloat(target)
  res.json({ ok: true, target })
})

// Tune amplifier layer
app.post('/api/amplifier/layer', (req, res) => {
  const { id, rate } = req.body
  try {
    const ok = _updateLayer(parseInt(id), parseFloat(rate))
    res.json({ ok, layers: _layerBreakdown() })
  } catch (e) {
    res.status(500).json({ error: e.message })
  }
})

// Toggle amplifier layer
app.post('/api/amplifier/toggle', (req, res) => {
  const { id, active } = req.body
  try {
    const ok = _updateLayer(parseInt(id), undefined, active)
    res.json({ ok, layers: _layerBreakdown() })
  } catch (e) {
    res.status(500).json({ error: e.message })
  }
})

// Pause executor
app.post('/api/executor/pause', (req, res) => {
  const H2 = hot()
  if (!H2) return res.status(503).json({ error: 'SAB not ready' })
  H2[H.GAS_OK] = 0
  res.json({ ok: true, status: 'paused' })
})

// Resume executor
app.post('/api/executor/resume', (req, res) => {
  const H2 = hot()
  if (!H2) return res.status(503).json({ error: 'SAB not ready' })
  H2[H.GAS_OK] = 1
  res.json({ ok: true, status: 'resumed' })
})

// Reset counters
app.post('/api/executor/reset', (req, res) => {
  const H2 = hot()
  if (!H2) return res.status(503).json({ error: 'SAB not ready' })
  H2[H.SWAPS_TODAY]   = 0
  H2[H.SUCCESS_TODAY] = 0
  H2[H.FAIL_TODAY]    = 0
  H2[H.EXEC_TODAY]    = 0
  H2[H.REV_TODAY]     = 0
  H2[H.NET_TODAY]     = 0
  res.json({ ok: true })
})

// Set gas cap
app.post('/api/executor/gascap', (req, res) => {
  const { gwei } = req.body
  const H2 = hot()
  if (!H2) return res.status(503).json({ error: 'SAB not ready' })
  if (!gwei || gwei <= 0) return res.status(400).json({ error: 'invalid gwei' })
  H2[H.GAS_CAP] = parseFloat(gwei)
  res.json({ ok: true, gwei })
})

// Set max concurrent
app.post('/api/executor/concurrent', (req, res) => {
  const { max } = req.body
  const H2 = hot()
  if (!H2) return res.status(503).json({ error: 'SAB not ready' })
  if (!max || max <= 0) return res.status(400).json({ error: 'invalid max' })
  H2[H.MAX_CONCURRENT] = parseInt(max)
  res.json({ ok: true, max })
})

// Force treasury reconcile
app.post('/api/treasury/reconcile', async (req, res) => {
  const H2 = hot()
  if (!H2) return res.status(503).json({ error: 'SAB not ready' })
  try {
    await reconcile(H2)
    res.json({ ok: true, balance: H2[H.VAULT_BALANCE] })
  } catch (e) {
    res.status(500).json({ error: e.message })
  }
})

// FTW — quote
app.post('/api/ftw/quote', (req, res) => {
  const { amount, network } = req.body
  if (!amount || amount <= 0) return res.status(400).json({ error: 'amount required' })
  try {
    res.json({ ...calcFee(parseFloat(amount), network || 'wave'), ts: Date.now() })
  } catch (e) {
    res.status(500).json({ error: e.message })
  }
})

// FTW — withdraw USDC → fiat
app.post('/api/ftw/withdraw', async (req, res) => {
  const { amount, type, phone, accountNumber, accountName, swiftCode, network, address } = req.body
  if (!amount || amount <= 0) return res.status(400).json({ error: 'amount required' })
  const key = process.env.MODEMPAY_SECRET_KEY || ''
  if (!key) return res.status(400).json({
    error: 'MODEMPAY_SECRET_KEY not configured in Railway env vars',
  })
  try {
    const result = await mpSend(key, {
      type, amount: parseFloat(amount),
      phone, accountNumber, accountName,
      swiftCode, network, address,
    })
    broadcast({ type: 'ftw', amount, network: result.network })
    res.json({ ok: true, ...result })
  } catch (e) {
    res.status(500).json({ error: e.message?.slice(0, 120) })
  }
})

// FTW — available networks
app.get('/api/ftw/networks', (_, res) => {
  try { res.json(networks()) } catch (e) { res.json([]) }
})

// ── WEBSOCKET ──────────────────────────────────────────────────────────────────
wss.on('connection', ws => {
  WS_CLIENTS.add(ws)
  // Send full state immediately on connect — don't wait for the next interval
  try { ws.send(JSON.stringify(fullState())) } catch {}
  ws.on('close', () => WS_CLIENTS.delete(ws))
  ws.on('error', () => WS_CLIENTS.delete(ws))
})

// ── EXPORT ────────────────────────────────────────────────────────────────────
export function startDashboard(SAB) {
  SAB_REF = SAB  // wire the SharedArrayBuffer — fullState() becomes live instantly
  srv.listen(PORT, () => {
    console.log(`[DASHBOARD] SOVEREIGN :${PORT} | 10 tabs | /ping`)
  })
}
