const element = id => document.getElementById(id);
const text = (id, value) => { element(id).textContent = value; };
const count = value => BigInt(value).toLocaleString('zh-CN');
const money = value => {
  const [whole, fraction = '00'] = value.split('.');
  return `${count(whole)}.${fraction.padEnd(2, '0')}`;
};
const date = value => new Date(value).toLocaleString('zh-CN', {hour12: false});
const localInput = value => {
  const time = new Date(value);
  return new Date(time.getTime() - time.getTimezoneOffset() * 60000).toISOString().slice(0, 16);
};
let settings = {dataset: 'retailpulse', top: '10', mode: 'recent'};
let timer;
let active;
let hasResults = false;

function table(id, rows) {
  const fragment = document.createDocumentFragment();
  for (const values of rows) {
    const row = document.createElement('tr');
    for (const value of values) {
      const cell = document.createElement('td');
      cell.textContent = value;
      row.append(cell);
    }
    fragment.append(row);
  }
  element(id).replaceChildren(fragment);
}

function clear() {
  for (const id of ['gmv', 'paid-orders', 'paid-users', 'refund-amount']) text(id, '—');
  text('refunds', '— 笔退款');
  text('window', '等待数据');
  for (const id of ['ranking', 'history']) {
    table(id, []);
    text(`${id}-empty`, '等待数据');
    element(`${id}-empty`).hidden = false;
  }
  element('truncated').hidden = true;
  text('updated', '尚未刷新');
  hasResults = false;
}

async function request(path, parameters, signal) {
  const response = await fetch(`/api/metrics/${path}?${new URLSearchParams(parameters)}`, {signal, cache: 'no-store'});
  if (response.status === 204) return null;
  if (!response.ok) {
    const problem = await response.json();
    throw new Error(problem.detail || `查询失败（${response.status}）`);
  }
  return response.json();
}

async function refresh() {
  clearTimeout(timer);
  active?.abort();
  const run = new AbortController();
  active = run;
  const timeout = setTimeout(() => run.abort(), 15000);
  text('status', '正在更新…');
  element('error').hidden = true;
  try {
    const current = {...settings};
    const latest = await request('latest', {dataset: current.dataset}, run.signal);
    let range;
    if (current.mode === 'custom') range = {from: current.from, to: current.to};
    else if (latest) range = {from: new Date(new Date(latest.windowEnd).getTime() - 3600000).toISOString(), to: latest.windowEnd};
    const [ranking, history] = await Promise.all([
      latest ? request('top-products', {dataset: current.dataset, windowStart: latest.windowStart,
        resultVersion: latest.resultVersion, limit: current.top}, run.signal) : {ready: false, products: []},
      range ? request('range', {dataset: current.dataset, ...range, limit: '1440'}, run.signal) : {items: [], truncated: false}
    ]);
    if (active !== run) return;
    if (latest) {
      text('gmv', money(latest.gmv));
      text('paid-orders', count(latest.paidOrders));
      text('paid-users', count(latest.paidUsers));
      text('refund-amount', money(latest.refundAmount));
      text('refunds', `${count(latest.refunds)} 笔退款`);
      const old = Date.now() - new Date(latest.windowEnd).getTime() > 300000;
      text('window', `${date(latest.windowStart)} — ${new Date(latest.windowEnd).toLocaleTimeString('zh-CN', {hour12: false})}${old ? ' · 非近期窗口' : ''}`);
    } else {
      clear();
      text('window', '此数据集尚无已完成的窗口');
    }
    table('ranking', ranking.products.map(p => [count(p.rank), p.productId, money(p.gmv), count(p.paidOrders), count(p.paidUsers), count(p.paidQuantity)]));
    text('ranking-empty', !latest ? '暂无窗口数据' : !ranking.ready ? '等待该窗口的榜单写入，稍后刷新。' : '该窗口的榜单为空。');
    element('ranking-empty').hidden = ranking.products.length > 0;
    table('history', history.items.map(m => [date(m.windowStart), money(m.gmv), count(m.paidOrders), count(m.paidUsers), count(m.paidQuantity), money(m.refundAmount), count(m.refunds)]));
    text('history-empty', '此时间范围暂无数据');
    element('history-empty').hidden = history.items.length > 0;
    element('truncated').hidden = !history.truncated;
    if (range && current.mode === 'recent') {
      element('from').value = localInput(range.from);
      element('to').value = localInput(range.to);
    }
    hasResults = true;
    text('status', latest ? '数据已更新' : '暂无数据');
    text('updated', `上次成功刷新 ${new Date().toLocaleTimeString('zh-CN', {hour12: false})}`);
  } catch (error) {
    if (active !== run) return;
    text('status', '刷新失败');
    text('error', `${error.name === 'AbortError' ? '请求超时，请稍后重试' : error.message}${hasResults ? '。以下保留上次成功查询的结果。' : ''}`);
    element('error').hidden = false;
  } finally {
    clearTimeout(timeout);
    if (active === run && element('auto-refresh').checked) timer = setTimeout(refresh, 10000);
  }
}

function apply(event) {
  event.preventDefault();
  const dataset = element('dataset').value.trim();
  element('dataset').setCustomValidity(dataset ? '' : '请输入数据集名称');
  if (!element('filters').reportValidity() || !element('range-filter').reportValidity()) return;
  const next = {dataset, top: element('top-limit').value, mode: element('range-mode').value};
  if (next.mode === 'custom') {
    const from = new Date(element('from').value);
    const to = new Date(element('to').value);
    if (!(from < to) || to - from > 86400000) {
      text('error', '开始时间必须早于结束时间，且范围不能超过 24 小时。');
      element('error').hidden = false;
      return;
    }
    next.from = from.toISOString();
    next.to = to.toISOString();
  }
  settings = next;
  clear();
  refresh();
}

element('filters').addEventListener('submit', apply);
element('range-filter').addEventListener('submit', apply);
element('dataset').addEventListener('input', () => element('dataset').setCustomValidity(''));
element('range-mode').addEventListener('change', () => {
  const custom = element('range-mode').value === 'custom';
  for (const id of ['from', 'to']) {
    element(id).disabled = !custom;
    element(id).required = custom;
  }
});
element('auto-refresh').addEventListener('change', () => {
  clearTimeout(timer);
  if (element('auto-refresh').checked) refresh();
});
refresh();
