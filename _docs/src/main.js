import { tools, groups, scenarios } from './tools.js'
import { configSections, runtime } from './config.js'

const $ = (sel, root = document) => root.querySelector(sel)

const el = (tag, attrs = {}, ...children) => {
  const node = document.createElement(tag)
  for (const [k, v] of Object.entries(attrs)) {
    if (v == null || v === false) continue
    if (k === 'class') node.className = v
    else if (k === 'html') node.innerHTML = v
    else if (k.startsWith('on')) node.addEventListener(k.slice(2), v)
    else node.setAttribute(k, v)
  }
  for (const child of children.flat(Infinity)) {
    if (child == null || child === false) continue
    node.append(child instanceof Node ? child : document.createTextNode(child))
  }
  return node
}

const escape = (s) => s.replace(/&/g, '&amp;').replace(/</g, '&lt;')

const highlightJSON = (value) =>
  escape(JSON.stringify(value, null, 2)).replace(
    /("(?:\\.|[^"\\])*")(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d+)?/g,
    (match, str, colon, lit) => {
      if (str) return colon ? `<span class="k">${str}</span>${colon}` : `<span class="s">${str}</span>`
      if (lit) return `<span class="v">${lit}</span>`
      return `<span class="n">${match}</span>`
    },
  )

const codeBlock = (value, label) => {
  const pre = el('pre', {}, el('code', { html: highlightJSON(value) }))
  addCopy(pre)
  return el('div', { class: 'io' }, el('div', { class: 'io-label' }, label), pre)
}

function addCopy(pre) {
  const btn = el('button', { class: 'copy', type: 'button' }, 'copy')
  btn.addEventListener('click', async () => {
    try {
      await navigator.clipboard.writeText(pre.querySelector('code').innerText)
      btn.textContent = 'copied'
    } catch {
      btn.textContent = 'failed'
    }
    setTimeout(() => (btn.textContent = 'copy'), 1200)
  })
  pre.append(btn)
}

// Tabs

document.querySelectorAll('[data-tabs]').forEach((box) => {
  box.addEventListener('click', (e) => {
    const btn = e.target.closest('button[data-tab]')
    if (!btn) return
    box.querySelectorAll('button[data-tab]').forEach((b) => b.classList.toggle('active', b === btn))
    box.querySelectorAll('[data-panel]').forEach((p) => p.classList.toggle('active', p.dataset.panel === btn.dataset.tab))
  })
})

document.querySelectorAll('pre').forEach(addCopy)

// Configuration reference

$('#config-ref').replaceChildren(
  ...configSections.map((section) =>
    el(
      'div',
      { class: 'config-block', id: `cfg-${section.id}` },
      el('div', { class: 'config-block-head' },
        el('h3', {}, section.title),
        el('p', {}, section.intro),
      ),
      el(
        'div',
        { class: 'table-wrap' },
        el(
          'table',
          { class: 'ref' },
          el('thead', {}, el('tr', {}, el('th', {}, 'field'), el('th', {}, 'type'), el('th', {}, 'default'), el('th', {}, 'meaning'))),
          el(
            'tbody',
            {},
            section.fields.map(([key, type, def, meaning]) =>
              el(
                'tr',
                {},
                el('td', { class: 'key' }, el('span', { class: 'key-path' }, section.path), key),
                el('td', { class: 'type' }, type),
                el('td', { class: def === 'required' ? 'def req' : 'def' }, def || '·'),
                el('td', {}, meaning),
              ),
            ),
          ),
        ),
      ),
    ),
  ),
)

$('#runtime-table tbody').replaceChildren(
  ...runtime.map(([name, kind, meaning]) =>
    el('tr', {}, el('td', { class: 'key' }, name), el('td', { class: 'type' }, kind), el('td', {}, meaning)),
  ),
)

// Tools

$('#tool-count').textContent = String(tools.length).padStart(2, '0')

const list = $('#tool-list')
const filters = $('#filters')
let active = 'all'

const renderFilters = () => {
  filters.replaceChildren(
    ...[['all', 'all'], ...Object.entries(groups)].map(([key, label]) => {
      const count = key === 'all' ? tools.length : tools.filter((t) => t.group === key).length
      return el(
        'button',
        {
          type: 'button',
          class: key === active ? 'active' : null,
          'aria-pressed': String(key === active),
          onclick: () => {
            active = key
            renderFilters()
            renderTools()
          },
        },
        label,
        el('span', { class: 'count' }, String(count).padStart(2, '0')),
      )
    }),
  )
}

const paramTable = (t) => {
  if (!t.params.length) return el('p', { class: 'no-params' }, 'Takes no parameters.')
  const rows = t.params.map(([name, type, required, meaning]) =>
    el(
      'tr',
      {},
      el('td', { class: 'key' }, name),
      el('td', { class: 'type' }, type),
      el('td', { class: required ? 'def req' : 'def' }, required ? 'required' : 'optional'),
      el('td', {}, meaning),
    ),
  )
  return el(
    'div',
    { class: 'table-wrap' },
    t.batch
      ? el('p', { class: 'param-note' },
          'Each entry in ', el('code', {}, 'items'), ` (1 to ${t.batch}) takes:`,
          t.write ? [' Top-level ', el('code', {}, 'confirm'), ' applies the whole batch; without it nothing changes.'] : null)
      : null,
    el('table', { class: 'ref' },
      el('thead', {}, el('tr', {}, el('th', {}, 'parameter'), el('th', {}, 'type'), el('th', {}, ''), el('th', {}, 'meaning'))),
      el('tbody', {}, rows)),
  )
}

const renderTools = () => {
  const visible = tools.filter((t) => active === 'all' || t.group === active)
  list.replaceChildren(
    ...visible.map((t) => {
      const index = tools.indexOf(t) + 1
      return el(
        'details',
        { class: `tool g-${t.group}`, id: `tool-${t.name}` },
        el(
          'summary',
          {},
          el('span', { class: 'tool-n' }, String(index).padStart(2, '0')),
          el('span', { class: 'tool-name' }, t.name),
          el('span', { class: 'tool-summary' }, t.summary),
          el('span', { class: 'tool-meta' },
            el('span', { class: 'tool-group' }, groups[t.group]),
            t.write === 'own' ? el('span', { class: 'flag warn' }, 'hidden when read-only') : null,
            t.write === 'destination' ? el('span', { class: 'flag warn' }, 'checks destination') : null,
          ),
        ),
        el(
          'div',
          { class: 'tool-body' },
          el('p', { class: 'tool-detail' }, t.detail),
          paramTable(t),
          el('div', { class: 'io-pair' },
            codeBlock({ name: t.name, arguments: t.call }, 'call'),
            codeBlock(t.result, 'response'),
          ),
        ),
      )
    }),
  )
}

renderFilters()
renderTools()

// Scenarios

$('#scenario-list').replaceChildren(
  ...scenarios.map((s, i) =>
    el(
      'article',
      { class: 'scenario' },
      el('span', { class: 'scenario-n' }, String(i + 1).padStart(2, '0')),
      el('div', { class: 'scenario-main' },
        el('p', { class: 'ask' }, el('span', { class: 'prompt-mark', 'aria-hidden': 'true' }, '> '), s.ask),
        el('p', { class: 'note' }, s.note),
      ),
      el('ol', { class: 'flow', 'aria-label': 'Tools called, in order' },
        s.flow.map((name) =>
          el('li', {}, el('a', { href: `#tool-${name}`, class: 'flow-tool', onclick: () => openTool(name) }, name)),
        ),
      ),
      el('a', {
        class: 'guide',
        href: `https://github.com/denizgursoy/kafka-mcp/blob/main/skills/kafka-debugging/references/${s.guide}.md`,
      }, `${s.guide}.md ↗`),
    ),
  ),
)

function openTool(name) {
  if (active !== 'all' && tools.find((t) => t.name === name)?.group !== active) {
    active = 'all'
    renderFilters()
    renderTools()
  }
  const node = document.getElementById(`tool-${name}`)
  if (node) node.open = true
}

const openFromHash = () => {
  const match = location.hash.match(/^#tool-(.+)$/)
  if (!match) return
  openTool(match[1])
  document.getElementById(`tool-${match[1]}`)?.scrollIntoView()
}
window.addEventListener('hashchange', openFromHash)
openFromHash()

// Hero session: a short investigation, replayed line by line

const session = [
  ['you', 'find the message for order ORD-12345 on orders'],
  ['call', 'sample_messages', 'items: [{ topic: "orders" }]'],
  ['ret', 'key_in_value: ["payload.orderId"]  → the key is the order id'],
  ['call', 'search_messages', 'script: "return key === \'ORD-12345\'"'],
  ['ret', 'match_count: 1   complete: true   scanned: 120000'],
  ['agent', 'orders / partition 3 / offset 48211, produced 14:02:17. The whole topic was scanned, so this is the only one.'],
]

const log = $('#session-log')
const reduced = window.matchMedia('(prefers-reduced-motion: reduce)').matches

const sessionLine = ([kind, a, b]) => {
  const time = el('span', { class: 'ts' })
  const body =
    kind === 'call'
      ? el('span', { class: 'body' }, el('strong', {}, a), ' ', el('span', { class: 'args' }, b))
      : el('span', { class: 'body' }, a)
  return el('li', { class: `line ${kind}` }, time, el('span', { class: 'who' }, kind), body)
}

const stamp = (node, seconds) => {
  const d = new Date(Date.UTC(2026, 0, 1, 14, 2, 10 + seconds))
  node.querySelector('.ts').textContent = d.toISOString().slice(11, 19)
}

const play = async () => {
  log.replaceChildren()
  let t = 0
  for (const entry of session) {
    const node = sessionLine(entry)
    stamp(node, t)
    t += entry[0] === 'call' ? 2 : 1
    log.append(node)
    if (reduced) continue
    await new Promise((r) => setTimeout(r, 30))
    node.classList.add('in')
    await new Promise((r) => setTimeout(r, entry[0] === 'call' ? 1000 : 750))
  }
  if (reduced) log.querySelectorAll('.line').forEach((n) => n.classList.add('in'))
  else setTimeout(play, 7000)
}

play()

// Hero headline: typed, held, erased, and replaced by the next question

const headlines = [
  'find the\nmessage',
  'measure\nthe lag',
  'skip the\npoison',
  'compare\nclusters',
  'free the\nconsumer',
]

const typer = $('.typer')
const typed = $('#typed')
const wait = (ms) => new Promise((r) => setTimeout(r, ms))

const typeLoop = async () => {
  if (reduced) return
  let i = 0
  await wait(2600)
  for (;;) {
    typer.classList.remove('idle')
    for (let n = typed.textContent.length; n >= 0; n--) {
      typed.textContent = typed.textContent.slice(0, n)
      await wait(38)
    }
    typer.classList.add('idle')
    await wait(380)
    typer.classList.remove('idle')
    i = (i + 1) % headlines.length
    const text = headlines[i]
    for (let n = 1; n <= text.length; n++) {
      typed.textContent = text.slice(0, n)
      await wait(text[n - 1] === '\n' ? 180 : 70 + Math.random() * 60)
    }
    typer.classList.add('idle')
    await wait(2800)
  }
}

typer.classList.add('idle')
typeLoop()
// Hero backdrop: a slow field of dots, drifting like partitions of a stream

const canvas = $('#field')
const ctx = canvas.getContext('2d')
let w = 0
let h = 0
let dpr = 1

const resize = () => {
  dpr = Math.min(window.devicePixelRatio || 1, 2)
  w = canvas.clientWidth
  h = canvas.clientHeight
  canvas.width = w * dpr
  canvas.height = h * dpr
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0)
}

const draw = (time) => {
  ctx.clearRect(0, 0, w, h)
  const lanes = 6
  const step = 9
  for (let lane = 0; lane < lanes; lane++) {
    const base = h * (0.22 + lane * 0.11)
    const amp = 26 + lane * 9
    const phase = time / 9000 + lane * 0.8
    for (let x = 0; x < w; x += step) {
      for (let k = 0; k < 3; k++) {
        const y = base + Math.sin(x / 260 + phase) * amp + Math.cos(x / 120 - phase * 1.4) * 10 + k * 7
        const fade = Math.max(0, Math.min(1, (x - w * 0.3) / (w * 0.4)))
        const alpha = 0.06 + 0.22 * fade * (1 - k * 0.3)
        ctx.fillStyle = `rgba(214, 205, 186, ${alpha})`
        ctx.fillRect(x + ((k * 3) % step), y, 1.4, 1.4)
      }
    }
  }
  if (!reduced) requestAnimationFrame(draw)
}

resize()
window.addEventListener('resize', () => {
  resize()
  if (reduced) draw(0)
})
requestAnimationFrame(draw)
