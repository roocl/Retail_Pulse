const $ = id => document.getElementById(id);
const labels = {SINGLE_PURCHASE:'单次购买',REPEAT_CUSTOMER:'复购客户',HIGH_VALUE:'高价值客户',AT_RISK:'待关注客户'};
let batch = null, next = null, generation = 0, controller, detailController, query;
const value = text => text == null ? '—' : String(text);
async function read(url, signal) {
  const response = await fetch(url,{signal});
  if (!response.ok) throw new Error(response.status === 404 ? '客户或画像批次不存在，或客户服务尚未启用。' : '客户数据读取失败，请稍后重试。');
  return response.json();
}
async function load(reset = true) {
  controller?.abort(); detailController?.abort(); controller = new AbortController(); const current = ++generation;
  if(reset){batch=null;next=null;}
  if(reset){
    query = new URLSearchParams({dataset:$('customer-dataset').value,limit:'25'});
    if($('customer-id').value)query.set('customerId',$('customer-id').value);
    if($('customer-segment').value)query.set('segment',$('customer-segment').value);
  }
  const params = new URLSearchParams(query);
  if(batch)params.set('batch',batch.id);
  if(next)params.set('after',next);
  $('customer-error').hidden=true; $('customer-status').textContent='正在读取画像…'; $('customer-next').disabled=true;
  try {
    const page=await read(`/api/customers?${params}`,controller.signal);
    if(current!==generation)return;
    batch=page.batch;next=page.nextAfter;
    $('customer-rows').replaceChildren();$('customer-summary').replaceChildren();
    $('customer-status').textContent=page.items.length ? `本页 ${page.items.length} 位客户` : '暂无符合条件的客户';
    $('batch-context').textContent=batch ? `观察日期 ${batch.observation} · 此前 ${batch.windowDays} 天 · 数据集 ${batch.dataset} · 批次 ${batch.id}` : '尚无已发布画像';
    for(const profile of page.items){
      const row=document.createElement('tr');
      for(const item of [profile.customerId,profile.country,`${profile.recencyDays} 天`,profile.orders,profile.purchaseAmount,labels[profile.segment]]){
        const cell=document.createElement('td');cell.textContent=value(item);row.append(cell);
      }
      const cell=document.createElement('td'),button=document.createElement('button');button.textContent='查看';
      const selectedBatch=batch;
      button.addEventListener('click',()=>detail(selectedBatch,profile.customerId));cell.append(button);row.append(cell);$('customer-rows').append(row);
    }
    $('customer-next').disabled=!page.hasMore;
    if(batch){
      const summary=await read(`/api/customers/summary?${new URLSearchParams({dataset:batch.dataset,batch:batch.id})}`,controller.signal);
      if(current!==generation)return;
      for(const group of summary){const article=document.createElement('article');article.className='metric';
        const title=document.createElement('span'),number=document.createElement('strong'),amount=document.createElement('small');
        title.textContent=labels[group.segment];number.textContent=`${group.customers} 人`;amount.textContent=`购买 ${group.purchase_amount} GBP`;
        article.append(title,number,amount);$('customer-summary').append(article);}
    }
  } catch(error){if(error.name!=='AbortError' && current===generation){$('customer-error').textContent=error.message;$('customer-error').hidden=false;$('customer-status').textContent='读取失败';}}
}
async function detail(selectedBatch,customerId){
  detailController?.abort(); detailController=new AbortController();
  try{
    const profile=await read(`/api/customers/${encodeURIComponent(customerId)}?${new URLSearchParams({dataset:selectedBatch.dataset,batch:selectedBatch.id})}`,detailController.signal);
    $('detail-fields').replaceChildren();$('detail-context').textContent=`${selectedBatch.dataset} · 观察日期 ${selectedBatch.observation}`;
    const fields={'客户编号':profile.customerId,'国家':profile.country,'标签':labels[profile.segment],'距最近购买':`${profile.recencyDays} 天`,'购买订单':profile.orders,'购买金额 GBP':profile.purchaseAmount,'取消金额 GBP':profile.cancellationAmount,'偏好商品编号':profile.preferredProduct,'R / F / M':`${profile.rScore} / ${profile.fScore} / ${profile.mScore}`};
    for(const [name,item] of Object.entries(fields)){const term=document.createElement('dt'),description=document.createElement('dd');term.textContent=name;description.textContent=value(item);$('detail-fields').append(term,description);}
    $('customer-detail').showModal();
  }catch(error){if(error.name!=='AbortError'){$('customer-error').textContent=error.message;$('customer-error').hidden=false;}}
}
$('customer-filters').addEventListener('submit',event=>{event.preventDefault();load();});
$('customer-first').addEventListener('click',()=>load());
$('customer-next').addEventListener('click',()=>load(false));
load();
