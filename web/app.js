const $=s=>document.querySelector(s);
const esc=s=>String(s==null?'':s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'})[c]);
const ETAGS={online:'上线',offline:'掉线',version_change:'版本变更',first_seen:'新设备'};
const SITE_COLORS=['#2563eb','#059669','#d97706','#7c3aed','#db2777','#0891b2'];
function siteColor(s){let i=0;for(const c of String(s))i=(i*31+c.charCodeAt(0))%997;return SITE_COLORS[i%SITE_COLORS.length]}
function fmtTime(u){return new Date(u*1000).toLocaleString('zh-CN',{hour12:false})}
async function copyText(text){
  try{await navigator.clipboard.writeText(text);return true}catch(e){}
  try{const ta=document.createElement('textarea');ta.value=text;ta.style.position='fixed';ta.style.opacity='0';document.body.appendChild(ta);ta.select();const ok=document.execCommand('copy');ta.remove();return ok}catch(e){return false}
}
function pct(v){return v==null?'—':(Math.round(v*1000)/10)+'%'}
function fmtDuration(m){if(m==null||isNaN(m))return '—';m=Math.round(m);if(m<=0)return '0分钟';const h=Math.floor(m/60),r=m%60;if(h&&r)return h+'小时'+r+'分';if(h)return h+'小时';return r+'分钟'}
function fmtHours(m){return m==null||isNaN(m)?'—':(m/60).toFixed(1)+'h'}
let fleet=[],sites=[],versions=[],sshHint={},q='',siteFilter='',verFilter='',targetVer='',pendingOnly=false,statusFilter='online',refreshInterval=15;
let collectDur={enabled:false,map:{}};
let webfix={map:{},cands:{}};
function matches(f){
  if(siteFilter&&(f.site||'未登记')!==siteFilter)return false;
  if(verFilter&&(f.version||'未获取')!==verFilter)return false;
  if(pendingOnly&&(f.version||'未获取')===targetVer)return false;
  if(q){const n=q.toLowerCase();if(!f.hostname.includes(n)&&!String(f.device_id||'').toLowerCase().includes(n)&&!String(f.device_name||'').toLowerCase().includes(n)&&!String(f.ip||'').includes(n)&&!String(f.hw_model||'').toLowerCase().includes(n))return false}
  if(statusFilter==='online'&&!f.online)return false;
  if(statusFilter==='offline'&&f.online)return false;
  return true;
}
function filtered(){return fleet.filter(matches)}
function renderSites(){
  const el=$('#sites');let h=`<button class="chip${siteFilter===''?' on':''}" data-site="">全部 ${fleet.length}</button>`;
  for(const s of sites){h+=`<button class="chip${siteFilter===s.site?' on':''}" data-site="${esc(s.site)}" style="--c:${siteColor(s.site)}">${esc(s.site)} <b>${s.online}/${s.expected}</b></button>`}
  const un=fleet.filter(f=>!f.site).length;
  if(un)h+=`<button class="chip${siteFilter==='未登记'?' on':''}" data-site="未登记" style="--c:#64748b">未登记 <b>${un}</b></button>`;
  el.innerHTML=h;
  el.querySelectorAll('.chip').forEach(b=>b.onclick=()=>{siteFilter=b.dataset.site===siteFilter?'':b.dataset.site;if(siteFilter&&statusFilter==='online'){statusFilter='all';$('#statusFilter').value='all'}renderSites();renderGrid()});
  $('#sitesN').textContent=sites.length;
  $('#missingN').textContent=fleet.filter(f=>!f.online).length;
}
function updateDrift(){
  const pend=fleet.filter(f=>!f.retired&&(f.version||'未获取')!==targetVer);
  $('#pendingN').textContent=pend.length;
  $('#targetLbl').textContent=targetVer?`待升级（≠ ${targetVer}）`:'待升级';
}
function renderVers(){
  const el=$('#vers');if(!el)return;
  let h=`<button class="chip${verFilter===''&&!pendingOnly?' on':''}" data-v="">全部 ${fleet.length}</button>`;
  for(const v of versions){
    const c=v.version==='未获取'?'#64748b':'#0f766e';
    const t=v.online!==v.count?`${v.online}/${v.count}`:String(v.count);
    h+=`<button class="chip${verFilter===v.version?' on':''}" data-v="${esc(v.version)}" style="--c:${c}">${esc(v.version)} <b>${t}</b></button>`;
  }
  if(targetVer){
    const pend=fleet.filter(f=>!f.retired&&(f.version||'未获取')!==targetVer).length;
    h+=`<button class="chip${pendingOnly?' on':''}" data-pending=1 style="--c:#b45309" title="只看待升级设备（≠ 目标版本）">≠ ${esc(targetVer)} <b>${pend}</b></button>`;
  }
  el.innerHTML=h;
  el.querySelectorAll('.chip').forEach(b=>b.onclick=()=>{
    if(b.dataset.pending){pendingOnly=!pendingOnly;if(pendingOnly)verFilter=''}
    else{verFilter=b.dataset.v===verFilter?'':b.dataset.v;pendingOnly=false}
    renderVers();renderGrid();
  });
  updateDrift();
}
function renderTargetSel(){
  const sel=$('#targetSel');if(!sel)return;
  const opts=versions.map(v=>v.version);
  sel.innerHTML=opts.map(v=>`<option value="${esc(v)}">${esc(v)}</option>`).join('');
  if(!targetVer||!opts.includes(targetVer))targetVer=opts.find(v=>v!=='未获取')||opts[0]||'';
  sel.value=targetVer;
}
/* ---- web 代理补配状态（KVWO-18，契约按 KVWO-19 feat/auto-webfix 定稿对齐）----
 GET /api/webfix/tasks → {enabled,auto_enabled,port_base,port_max,verify_timeout,
 candidates:[{hostname,web_port}],counts,tasks:[{task_id,hostname,site,trigger_type,
 status queued|running|success|failed,error,attempts,web_port,created_at(Unix 秒),
 finished_at,chars}]}（tasks 为每设备最新一行；candidates 为当前缺配候选）。
 POST /api/webfix/retry {devices:[...]} → {ok,started:[{hostname,task_id,attempts}],
 skipped:[{hostname,reason}]}（skipped 必须向用户呈现原因）；SSE 复用
 /api/upgrade/events：type=webfix（task 同上字段）/ webfix_log（日志增量，总览不消费）。
 与 /api/hosts 的 web_port 组合判定：有 web_port → 正常（无标记）；在线 + 有 ssh_port + 无
 web_port → 缺配候选，再叠加最新任务状态：queued/running → 补配中，failed → 失败（悬停看
 error），success → 标记消失（版本随下次 /api/hosts 刷新恢复显示）。
 接口未上线（404）或拉取失败时优雅降级：沿用上次数据，从无数据时仅按 web_port 显示灰色
「web 缺配」标记，不显示进行中/失败态。字段解析保留容错（数组/字典形态、ISO/Unix 时间）。 */
function webfixTask(f){return (webfix.map||{})[String((f&&(f.hostname||f.host))||'').toLowerCase()]||null}
function webfixState(f){
  const t=webfixTask(f);
  if(f&&f.web_port)return{code:'normal',task:t};
  if(!(f&&f.online&&f.ssh_port))return{code:'',task:t};
  const s=t?String(t.status||'').toLowerCase():'';
  if(s==='success'||s==='done'||s==='ok')return{code:'',task:t};
  if(s==='queued'||s==='running'||s==='pending')return{code:'running',task:t};
  if(s==='failed'||s==='fail'||s==='error')return{code:'failed',title:(t&&t.error)||'补配失败（未返回原因）',task:t};
  return{code:'missing',task:t};
}
function webfixBadge(f){
  const st=webfixState(f);
  if(st.code==='running')return '<span class="wfix running" title="web 代理补配进行中…"><i class="wspin"></i>补配中…</span>';
  if(st.code==='failed')return `<span class="wfix failed" title="${esc(st.title)}">补配失败</span>`;
  if(st.code==='missing')return '<span class="wfix" title="未检测到 web 代理（ep-编号-web），等待自动补配">web 缺配</span>';
  return '';
}
function card(f){
  const tag=f.site?`<span class="stag" style="--c:${siteColor(f.site)}">${esc(f.site)}</span>`:'';
  const wtag=webfixBadge(f);
  const cd=collectDur.enabled?collectDur.map[(f.hostname||'').toLowerCase()]:null;
  if(f.online){
    return `<article class="card online" data-h="${esc(f.hostname)}"><div class="top"><span class="name"><i class="dot"></i>${esc(f.hostname)}</span>${tag}${wtag}</div>
    <div class="row"><span>版本</span><span class="value">${esc(f.version||'未获取')}${f.version_error?` <small title="${esc(f.version_error)}">(读取失败)</small>`:''}</span></div>
    <div class="row"><span>硬件</span><span class="value">${esc(f.hardware||'—')}${f.hw_model?` · ${esc(f.hw_model)}`:''}</span></div>
    <div class="row"><span>IP</span><span class="value">${esc(f.ip||'—')}</span></div>
    <div class="row"><span>SSH / Web</span><span class="value">${f.ssh_port||'—'} / ${f.web_port||'—'}</span></div>
    <div class="row"><span>device_id</span><span class="value">${esc(f.device_id||'—')}</span></div>
    <div class="row"><span>连接数</span><span class="value">${f.connections!=null?f.connections:'—'}</span></div>
    ${collectDur.enabled?`<div class="row"><span>今日采集</span><span class="value">${cd&&cd.today_minutes!=null?fmtHours(cd.today_minutes):'—'}</span></div>`:''}
    <div class="row"><small>frpc 上线：${esc(f.started||'—')}</small></div></article>`;
  }
  return `<article class="card offline" data-h="${esc(f.hostname)}"><div class="top"><span class="name"><i class="dot off"></i>${esc(f.hostname)}</span>${tag}</div>
  <div class="row"><span>状态</span><span class="value bad">未上线</span></div>
  <div class="row"><span>最后在线</span><span class="value">${f.last_seen?esc(f.last_seen):'从未连接'}</span></div>
  <div class="row"><span>硬件</span><span class="value">${esc(f.hardware||'—')}${f.hw_model?` · ${esc(f.hw_model)}`:''}</span></div>
  <div class="row"><span>IP</span><span class="value">${esc(f.ip||'—')}</span></div>
  <div class="row"><span>device_id</span><span class="value">${esc(f.device_id||'—')}</span></div>
  <div class="row"><span>历史版本</span><span class="value">${esc(f.version||'—')}</span></div></article>`;
}
function renderGrid(){
  const list=fleet.filter(matches);
  $('#grid').innerHTML=list.length?list.map(card).join(''):'<div class="empty">没有匹配的设备</div>';
  document.querySelectorAll('.card').forEach(c=>c.onclick=()=>openDevice(c.dataset.h));
}
async function load(force){
  try{
    const r=await fetch('/api/hosts'+(force?'?refresh=1':''),{cache:'no-store'});
    const d=await r.json();
    if(!r.ok)throw Error(d.error||'请求失败');
    fleet=d.fleet||[];sites=d.sites||[];versions=d.versions||[];sshHint=d.ssh_hint||{};
    if(d.refresh_interval)refreshInterval=d.refresh_interval;
    $('#online').textContent=d.online;
    $('#registered').textContent=d.registered!=null?d.registered:d.total;
    $('#sub').textContent='最后更新：'+fmtTime(d.timestamp)+' · '+d.online+' 在线 / '+(d.registered!=null?d.registered:d.total)+' 台登记 · 每 '+refreshInterval+'s 自动刷新';
    renderSites();renderTargetSel();renderVers();renderGrid();
  }catch(e){$('#sub').textContent='获取 frps 状态失败：'+e}
}
function closeModal(){$('#modal').classList.remove('show')}
async function openDevice(h){
  const m=$('#modal');m.classList.add('show');$('#modalBody').innerHTML='<p class="eempty">加载中…</p>';
  try{
    const r=await fetch('/api/device/'+encodeURIComponent(h));const d=await r.json();
    if(!r.ok)throw Error(d.error||'读取失败');
    const hint=d.ssh_hint||sshHint||{};
    const cd=(collectDur.map||{})[String(d.hostname||'').toLowerCase()]||null;
    const cdRows=cd&&Array.isArray(cd.history)?cd.history.slice().sort((a,b)=>String(b.date||'').localeCompare(String(a.date||''))).map(r=>`<tr><td>${esc(r.date)}</td><td>${fmtDuration(r.minutes)}</td></tr>`).join(''):'';
    const evRows=(d.events||[]).map(e=>`<div class="erow"><span class="etag ${esc(e.event_type)}">${ETAGS[e.event_type]||esc(e.event_type)}</span><b>${esc(e.ts||'')}</b><span class="edetail">${esc(e.new_value||e.detail||'')}${e.old_value&&e.new_value?' ← '+esc(e.old_value):''}</span></div>`).join('')||'<p class="eempty">暂无事件</p>';
    const stRows=(d.status||[]).map(s=>`<tr><td>${esc(s.ts)}</td><td>${s.online?'<span style="color:#16a34a">在线</span>':'<span style="color:#dc2626">离线</span>'}</td><td>${esc(s.release_version||'—')}</td><td>${s.connections!=null?s.connections:'—'}</td></tr>`).join('');
    const cmd=d.ssh_port?`ssh -J ${hint.jump||'ubuntu@124.222.147.63'} -p ${d.ssh_port} ${hint.user||'orangepi'}@127.0.0.1`:'';
    const wf=webfixState(d);
    const wfc=(webfix.cands||{})[String(d.hostname||d.host||'').toLowerCase()]||null;
    const wfExp=(wfc&&wfc.web_port)||(wf.task&&wf.task.web_port)||null;
    const wfRow=wf.code==='normal'?`${d.web_port||'—'} / <span style="color:#16a34a">正常</span>`
      :wf.code==='running'?`${d.web_port||'—'} / <span style="color:#1d4ed8"><i class="wspin"></i> 补配中</span>`
      :wf.code==='failed'?`${d.web_port||'—'} / <span style="color:#dc2626" title="${esc(wf.title)}">失败：${esc(wf.title)}</span>`
      :wf.code==='missing'?`${d.web_port||'—'} / <span style="color:#667085" title="未检测到 web 代理（ep-编号-web），等待自动补配">${wfExp?`缺配（补配后 ${esc(String(wfExp))}）`:'缺配'}</span>`
      :`${d.web_port||'—'} / <span style="color:#98a2b3" title="设备未在线或无 SSH 隧道时不判定缺配">—</span>`;
    const wfBar=wf.code==='failed'&&d.online&&d.ssh_port?`<div class="wfixbar"><small>自动补配仅触发一次；重试将重新推送 frpc 配置并重启该机 frpc（隧道秒级闪断）</small><button type="button" id="webfixRetry">重试补配</button></div>`:'';
    $('#modalBody').innerHTML=`
      <header><div><span class="name"><i class="dot${d.online?'':' off'}"></i>${esc(d.hostname)}</span>${d.site?` <span class="stag" style="--c:${siteColor(d.site)}">${esc(d.site)}</span>`:''}</div><button class="x" id="mClose">✕</button></header>
      <div class="mgrid">
        <span>设备名 / device_id</span><b>${esc(d.device_name||'—')} / ${esc(d.device_id||'—')}</b>
        <span>归属</span><b>${esc(d.owner||d.site||'未登记')}</b>
        <span>硬件版本</span><b>${esc(d.hardware||'—')}</b>
        <span>型号 / IP</span><b>${esc(d.hw_model||'—')}${d.ip?' / '+esc(d.ip):''}</b>
        <span>当前状态</span><b>${d.online?'在线':'未上线（最后在线 '+(d.last_seen?esc(d.last_seen):'从未')+'）'}</b>
        <span>版本</span><b>${esc(d.release_version||'未获取')}</b>
        <span>SSH / Web 端口</span><b>${d.ssh_port||'—'} / ${d.web_port||'—'}</b>
        <span>web 代理</span><b>${wfRow}</b>
        <span>首次 / 最后在线</span><b>${esc(d.first_seen||'—')} / ${esc(d.last_seen||'从未')}</b>
        <span>在线率 24h / 7d</span><b>${pct(d.uptime_24h)} / ${pct(d.uptime_7d)}</b>
        <span>采集时长</span><b>${fmtDuration(cd?cd.today_minutes:null)}</b>
      </div>
      ${wfBar}
      ${cmd?`<div class="cmd"><code id="sshCmd">${esc(cmd)}</code><button id="copyBtn">复制跳板命令</button></div>`:''}
      <h3>近 7 天采集时长</h3><div class="twrap"><table><tr><th>日期</th><th>时长</th></tr>${cdRows||`<tr><td colspan=2 class="eempty">${collectDur.enabled?'暂无采集记录':'采集时长统计未启用'}</td></tr>`}</table></div>
      <h3>最近快照（5 分钟一条）</h3><div class="twrap"><table><tr><th>时间</th><th>状态</th><th>版本</th><th>连接</th></tr>${stRows||'<tr><td colspan=4 class="eempty">暂无快照</td></tr>'}</table></div>
      <h3>最近事件</h3><div class="elist">${evRows}</div>`;
    $('#mClose').onclick=closeModal;
    const cb=$('#copyBtn');if(cb)cb.onclick=async()=>{if(await copyText(cmd)){cb.textContent='已复制 ✓';setTimeout(()=>cb.textContent='复制跳板命令',1500)}else alert(cmd)};
    const rb=$('#webfixRetry');
    if(rb)rb.onclick=async()=>{
      if(rb.dataset.arm!=='1'){rb.dataset.arm='1';rb.classList.add('arm');rb.textContent='确认重试？';setTimeout(()=>{if(rb.isConnected&&rb.dataset.arm==='1'){rb.dataset.arm='';rb.classList.remove('arm');rb.textContent='重试补配'}},4000);return}
      rb.dataset.arm='';rb.classList.remove('arm');
      rb.disabled=true;rb.textContent='发起中…';
      const wfail=msg=>{rb.disabled=false;rb.classList.remove('arm');rb.classList.add('err');rb.textContent='未发起：'+msg;setTimeout(()=>{if(rb.isConnected&&!rb.disabled){rb.classList.remove('err');rb.textContent='重试补配'}},8000)};
      try{
        const r=await fetch('/api/webfix/retry',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({devices:[d.hostname]})});
        const j=await r.json().catch(()=>({}));
        if(!r.ok||j.ok===false)throw Error(j.error||('HTTP '+r.status));
        const wnorm=x=>String((x&&(x.hostname||x.host))||'').toLowerCase();
        const me=String(d.hostname||'').toLowerCase();
        const wstarted=(j.started||[]).some(x=>wnorm(x)===me);
        const wskip=(j.skipped||[]).find(x=>wnorm(x)===me);
        if(!wstarted)throw Error(String((wskip&&(wskip.reason||wskip.error))||'服务端未确认该设备（响应无 started 记录）'));
        rb.classList.remove('arm');rb.textContent='已发起重试 ✓';
        setTimeout(()=>{loadWebfix();if($('#modal').classList.contains('show'))openDevice(d.hostname)},900);
      }catch(e){wfail(String(e.message||e))}
    };
  }catch(e){$('#modalBody').innerHTML='<p class="eempty">'+esc(String(e))+'</p>'}
}
$('#refresh').onclick=()=>load(true);
$('#q').oninput=e=>{q=e.target.value.trim().toLowerCase();renderGrid()};
$('#statusFilter').onchange=e=>{statusFilter=e.target.value;renderGrid()};
$('#targetSel').onchange=e=>{targetVer=e.target.value;renderVers();renderGrid()};
async function copyBtn(btn,text,label,okLabel){
  if(!text){btn.textContent='无匹配设备';setTimeout(()=>btn.textContent=label,1200);return}
  if(await copyText(text)){btn.textContent=okLabel}else{alert(text)}
  setTimeout(()=>btn.textContent=label,1800);
}
$('#copyPending').onclick=async()=>{
  const rows=filtered().filter(f=>!f.retired);
  const head=['设备号','站点','IP','SSH端口','device_id','当前版本','硬件'];
  const text=[head.join('\t')].concat(rows.map(f=>[f.hostname.toUpperCase(),f.site||'未登记',f.ip||'',f.ssh_port||'',f.device_id||'',f.version||'未获取',[f.hardware,f.hw_model].filter(Boolean).join(' ')||''].join('\t'))).join('\n');
  await copyBtn($('#copyPending'),rows.length?text:'','复制待升级清单',`已复制 ${rows.length} 台 ✓`);
};
$('#copyCmds').onclick=async()=>{
  const hint=sshHint||{},jump=hint.jump||'ubuntu@124.222.147.63',user=hint.user||'orangepi';
  const rows=filtered().filter(f=>!f.retired&&f.ssh_port);
  const text=rows.map(f=>`ssh -J ${jump} -p ${f.ssh_port} ${f.ssh_user||user}@127.0.0.1  # ${f.hostname.toUpperCase()}${f.site?' ('+f.site+')':''}`).join('\n');
  await copyBtn($('#copyCmds'),rows.length?text:'','复制跳板命令',`已复制 ${rows.length} 台 ✓`);
};
$('#copyList').onclick=async()=>{
  const list=filtered().map(f=>f.hostname.toUpperCase());
  const btn=$('#copyList');
  if(!list.length){btn.textContent='无匹配设备';setTimeout(()=>btn.textContent='复制编号',1200);return}
  if(await copyText(list.join('\n'))){btn.textContent=`已复制 ${list.length} 台 ✓`}else{alert(list.join('\n'))}
  setTimeout(()=>btn.textContent='复制编号',1800);
};
$('#modal').onclick=e=>{if(e.target.id==='modal')closeModal()};
document.addEventListener('keydown',e=>{if(e.key==='Escape')closeModal()});
load();setInterval(()=>load(false),15000);
async function loadEvents(){const el=document.querySelector('#eventsBody');if(!el)return;try{const r=await fetch('/api/events?hours=24&limit=60',{cache:'no-store'});const d=await r.json();if(!d.enabled){el.innerHTML='<p class=eempty>历史持久化未启用（数据库不可用）</p>';return}el.innerHTML=d.events.length?d.events.map(e=>`<div class=erow><span class="etag ${esc(e.event_type)}">${ETAGS[e.event_type]||esc(e.event_type)}</span><b>${esc(e.hostname)}</b><span class=edetail>${esc(e.new_value||e.detail||'')}${e.old_value&&e.new_value?' ← '+esc(e.old_value):''}</span><time>${esc(e.ts)}</time></div>`).join(''):'<p class=eempty>24 小时内暂无事件</p>'}catch(err){el.innerHTML='<p class=eempty>事件读取失败</p>'}}
loadEvents();setInterval(loadEvents,60000);
async function loadCollectDuration(){
  try{
    const r=await fetch('/api/collect-duration?days=7',{cache:'no-store'});
    const d=await r.json();
    if(!r.ok)throw Error(d.error||'请求失败');
    collectDur={enabled:!!d.enabled,map:{}};
    for(const it of d.items||[])collectDur.map[String(it.hostname||'').toLowerCase()]=it;
  }catch(e){/* 接口未上线或拉取失败:沿用已有数据,无数据时展示 — / 空态 */}
  if(fleet.length)renderGrid();
}
loadCollectDuration();setInterval(loadCollectDuration,300000);
async function loadWebfix(){
  try{
    const r=await fetch('/api/webfix/tasks',{cache:'no-store'});
    const d=await r.json();
    if(!r.ok)throw Error(d.error||('HTTP '+r.status));
    let arr=Array.isArray(d)?d:(d.tasks||d.items||d.devices||d.list||[]);
    if(!Array.isArray(arr)&&arr&&typeof arr==='object')arr=Object.entries(arr).map(([h,t])=>Object.assign({hostname:h},(t&&typeof t==='object')?t:{status:t}));
    const tsOf=v=>{if(typeof v==='number')return v>1e12?v:Math.round(v*1000);const p=Date.parse(String(v==null?'':v).replace(' ','T'));return isNaN(p)?0:p};
    const map={};
    for(const t of (arr||[])){
      const h=String((t&&(t.hostname||t.host))||'').toLowerCase();if(!h)continue;
      const ts=tsOf(t&&(t.created_at||t.created));
      const old=map[h];if(old&&ts<old._ts)continue;
      map[h]={hostname:h,status:t.status,error:t.error,attempts:t.attempts,trigger_type:t.trigger_type,web_port:t.web_port,created_at:t.created_at,finished_at:t.finished_at,_ts:ts};
    }
    const cands={};for(const c of (d.candidates||[]))if(c&&c.hostname)cands[String(c.hostname).toLowerCase()]=c;
    webfix={map:map,cands:cands};
  }catch(e){/* webfix 接口未上线/拉取失败：沿用已有数据；从无数据时退化为仅「web 缺配」灰色标记 */}
  if(fleet.length)renderGrid();
}
loadWebfix();setInterval(loadWebfix,15000);

/* ================= 升级管理视图（Upg.* 独立命名空间，不改动设备总览逻辑） =================
 数据契约（后端返工后联调基准）：POST /api/upgrade/jobs {kind,devices,params{version,oss_url,sha256}}、
 GET /api/upgrade/jobs(/<id>)、GET /api/upgrade/events（SSE：{type:'job'|'task'|'log'|'webfix'|'webfix_log',...}，
 webfix 类事件只驱动设备总览刷新、不进升级视图）、
 GET /api/upgrade/jobs/<id>/log?offset=N[&hostname=]、GET /api/upgrade/packages（包注册表）、
 POST/GET /api/upgrade/sha256（服务端一次性拉包算哈希，异步轮询）。旧 /api/packages 整套已不再依赖。
 字段解析全部做了兼容容错，后端未上线时各区城优雅空态；最终契约以胡磊涛完成评论为准。 */
const Upg={
STAGES:['precheck','download','prepare','install','verify','cleanup'],
STAGE_LBL:{precheck:'预检',download:'下载',prepare:'准备',install:'安装',verify:'验证',cleanup:'清理'},
KIND_LBL:{upgrade:'全流程升级',precheck:'仅预检',download:'仅下载',prepare:'仅准备',install:'仅安装',verify:'仅验证',cleanup:'仅清理'},
STAT_LBL:{running:'进行中',success:'成功',failed:'失败',blocked:'已阻断',pending:'等待中'},
fleet:[],selected:new Set(),selTouched:false,targetVersion:'',reg:[],regFail:false,
param:{url:'',ok:false,filename:'',version:'',arch:'',expiresAt:null,sha256:'',shaSrc:'',size:null},
shaComputing:false,shaPollT:null,shaPollN:0,urlTimer:null,
job:null,tasks:[],logs:{},logOff:{},follow:{},expanded:null,fromSnap:{},
es:null,pollT:null,connState:'off',view:'overview',stickyNote:null,
gid(i){return document.getElementById(i)},
normStatus(s){s=String(s==null?'':s).toLowerCase();
 if(['success','done','ok','complete','completed','finished'].includes(s))return 'success';
 if(['failed','fail','error','err'].includes(s))return 'failed';
 if(['blocked','block','skipped','skip'].includes(s))return 'blocked';
 if(['running','run','active','in_progress','progress','started'].includes(s))return 'running';
 return 'pending'},
active(){return !!this.job&&['running','pending'].includes(this.normStatus(this.job.status))},
fmtB(n){if(n==null||isNaN(n))return '—';const u=['B','KB','MB','GB','TB'];let i=0;n=Number(n);while(n>=1024&&i<u.length-1){n/=1024;i++}return(i?n.toFixed(1):Math.round(n))+' '+u[i]},
parseTs(v){if(v==null)return null;if(typeof v==='number')return v<1e12?v*1000:v;const t=Date.parse(String(v).replace(' ','T'));return isNaN(t)?null:t},
fmtClock(ms){const d=new Date(ms);return isNaN(d.getTime())?String(ms==null?'—':ms):d.toLocaleString('zh-CN',{hour12:false})},
fmtDur2(sec){if(sec==null||isNaN(sec)||sec<0)return '—';sec=Math.round(sec);const h=Math.floor(sec/3600),m=Math.floor(sec%3600/60),s=sec%60;if(h)return h+'时'+m+'分';if(m)return m+'分'+s+'秒';return s+'秒'},
verCmp(a,b){const pa=String(a).split(/[.-]/),pb=String(b).split(/[.-]/);for(let i=0;i<Math.max(pa.length,pb.length);i++){const x=parseInt(pa[i]),y=parseInt(pb[i]);if(!isNaN(x)&&!isNaN(y)){if(x!==y)return x<y?-1:1}else{const c=String(pa[i]||'').localeCompare(String(pb[i]||''));if(c)return c<0?-1:1}}return 0},
archOf(f){const m=String((f&&(f.hw_model||f.hardware))||'').toLowerCase();if(/rk3588|rk3576|rk3568|arm64|aarch64|orange\s?pi|v1/.test(m))return 'rk3588-arm64';if(/amd64|x86|intel|celeron|n100|v0/.test(m))return 'amd64';return ''},
init(){
 const tabs=this.gid('viewTabs');
 tabs.addEventListener('click',e=>{const b=e.target.closest('button[data-view]');if(b)this.setView(b.dataset.view)});
 window.addEventListener('hashchange',()=>{const h=new URLSearchParams(location.hash.slice(1)).get('view');const v=h==='upgrade'?'upgrade':(h==='assets'||h==='repairs')?'assets':'overview';if(v!==this.view)this.setView(v,true)});
 const urlIn=this.gid('upgUrl'),shaIn=this.gid('upgSha');
 urlIn.addEventListener('input',()=>{clearTimeout(this.urlTimer);this.urlTimer=setTimeout(()=>this.onUrlInput(),400)});
 urlIn.addEventListener('change',()=>{clearTimeout(this.urlTimer);this.onUrlInput()});
 shaIn.addEventListener('input',()=>{const v=shaIn.value.trim().toLowerCase();this.param.sha256=/^[0-9a-f]{64}$/.test(v)?v:'';this.param.shaSrc=this.param.sha256?'manual':'';this.renderParam()});
 this.gid('upgShaCompute').onclick=()=>this.computeSha();
 this.gid('upgParamSummary').addEventListener('click',async e=>{const b=e.target.closest('button[data-sha]');if(!b)return;b.textContent='…';const ok=await copyText(b.dataset.sha);b.textContent=ok?'已复制':'失败';setTimeout(()=>b.textContent='复制',1500)});
 const dg=this.gid('upgDevGroups');
 dg.addEventListener('change',e=>{const el=e.target;
  if(el.dataset.h!==undefined&&el.type==='checkbox'){this.selTouched=true;if(el.checked)this.selected.add(el.dataset.h);else this.selected.delete(el.dataset.h);this.renderDevs();this.updateSelBar();this.updateActions()}
  else if(el.dataset.grp!==undefined){this.selTouched=true;const rows=this.devRows().filter(r=>r.ver===el.dataset.grp&&r.online);for(const r of rows){if(el.checked)this.selected.add(r.h);else this.selected.delete(r.h)}this.renderDevs();this.updateSelBar();this.updateActions()}});
 dg.addEventListener('click',e=>{const b=e.target.closest('button[data-grpup]');if(!b)return;const k=b.dataset.grpup;this.selTouched=true;const rows=this.devRows().filter(r=>r.ver===k&&r.online);this.selected=new Set(rows.map(r=>r.h));this.renderDevs();this.updateSelBar();this.updateActions();this.setActHint('已选择 '+k+' 全组 '+rows.length+' 台，确认无误后在下方发起',false);this.gid('upgActPanel').scrollIntoView({behavior:'smooth',block:'center'})});
 this.gid('upgSelBar').addEventListener('click',e=>{if(e.target.closest('button[data-clr]')){this.selected.clear();this.selTouched=true;this.renderDevs();this.updateSelBar();this.updateActions()}});
 this.gid('upgRunAll').onclick=()=>{if(this.canLaunch())this.showConfirm()};
 document.querySelectorAll('.stagebtn[data-kind]').forEach(b=>b.onclick=()=>{if(this.canLaunch())this.launch(b.dataset.kind)});
 const grid=this.gid('upgTaskGrid');
 grid.addEventListener('click',e=>{
  const fb=e.target.closest('button[data-fl]');if(fb){const h=fb.dataset.fl;const cur=this.follow[h]!==false;this.follow[h]=!cur;fb.textContent=this.follow[h]===false?'恢复跟随':'暂停跟随';if(this.follow[h]!==false){const pre=this.logDom(h);if(pre)pre.scrollTop=pre.scrollHeight}return}
  const c=e.target.closest('.ucard');if(c)this.toggleLog(c.dataset.h)});
 const um=this.gid('upgModal');um.addEventListener('click',e=>{if(e.target===um)this.closeConfirm()});
 document.addEventListener('keydown',e=>{if(e.key==='Escape')this.closeConfirm()});
 this.route();
 setInterval(()=>{if(this.view==='upgrade')this.loadFleet()},15000);
},
setView(v,skipHash){
 const rep=v==='repairs';if(rep)v='assets';   // 全站唯一维修入口在资产区「维修看板」子页签；#view=repairs 深链兼容（KVWO-88）
 if(v!=='upgrade'&&v!=='assets')v='overview';
 this.view=v;
 this.gid('viewOverview').hidden=v!=='overview';
 this.gid('viewUpgrade').hidden=v!=='upgrade';
 this.gid('viewAssets').hidden=v!=='assets';
 document.querySelectorAll('#viewTabs button').forEach(b=>b.classList.toggle('on',b.dataset.view===v));
 if(!skipHash){const p=new URLSearchParams(location.hash.slice(1));if(v!=='overview'){p.set('view',v);if(rep)p.set('atab','repairs')}else p.delete('view');const s=p.toString();history.replaceState(null,'',location.pathname+location.search+(s?'#'+s:''))}
 if(v==='upgrade')this.enter();
 if(v==='assets'&&typeof Assets!=='undefined')Assets.enter();   // 资产区域（KVWO-84）懒加载，见文件尾 Assets.*（声明先于本行求值）
},
route(){const h=new URLSearchParams(location.hash.slice(1)).get('view');const v=h==='upgrade'?'upgrade':(h==='assets'||h==='repairs')?'assets':'overview';this.setView(v,true)},
enter(){this.loadRegistry();this.loadFleet();this.loadJobs();this.renderParam();this.updateSelBar();this.updateActions()},
setActHint(msg,warn){const el=this.gid('upgActHint');el.textContent=msg||'';el.className='uhint'+(warn?' warn':'')},
parseOssUrl(u){
 u=String(u||'').trim();
 const out={url:u,ok:false,filename:'',version:'',arch:'',expiresAt:null,err:'',sha256:'',shaSrc:'',size:null};
 if(!u)return out;
 let p=null;try{p=new URL(u)}catch(e){out.err='URL 格式无法解析';return out}
 out.ok=true;
 const segs=p.pathname.split('/').filter(Boolean);
 try{out.filename=decodeURIComponent(segs[segs.length-1]||'')}catch(e){out.filename=segs[segs.length-1]||''}
 const vm=out.filename.match(/v\d+\.\d+(\.\d+)*/i);out.version=vm?vm[0].toLowerCase():'';
 if(/rk3588|arm64|aarch64/i.test(out.filename))out.arch='rk3588-arm64';
 else if(/amd64|x86/i.test(out.filename))out.arch='amd64';
 const v1=parseInt(p.searchParams.get('Expires')||'',10);
 if(!isNaN(v1)&&v1>1000000)out.expiresAt=v1*1000;
 else{
  const d=p.searchParams.get('x-oss-date')||'',dur=parseInt(p.searchParams.get('x-oss-expires')||'',10);
  const m=d.match(/^(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})Z$/);
  if(m&&!isNaN(dur)){const t=Date.parse(m[1]+'-'+m[2]+'-'+m[3]+'T'+m[4]+':'+m[5]+':'+m[6]+'Z');if(!isNaN(t))out.expiresAt=t+dur*1000}
 }
 return out;
},
fmtRemain(ms){
 if(ms==null||isNaN(ms))return '—';
 if(ms<=0)return '已过期';
 const h=ms/3600000;
 if(h>=48)return Math.floor(h/24)+' 天';
 if(h>=1)return h.toFixed(1)+' 小时';
 return Math.max(1,Math.round(ms/60000))+' 分钟';
},
estMs(){return Math.max(1,Math.ceil((this.selected.size||1)/6))*12*60000},
onUrlInput(){
 const url=this.gid('upgUrl').value;
 const prev=this.param.filename;
 this.param=this.parseOssUrl(url);
 this.targetVersion=this.param.version;
 this.shaComputing=false;this.stopShaPoll();
 if(this.param.ok&&this.param.filename&&prev!==this.param.filename){
  if(this.reg.length)this.matchRegistry();
  else this.loadRegistry();
 }
 this.renderParam();
},
async loadRegistry(){
 try{
  const r=await fetch('/api/upgrade/packages',{cache:'no-store'});
  const d=await r.json();
  if(!r.ok)throw Error(d.error||('HTTP '+r.status));
  this.reg=d.packages||d.items||(Array.isArray(d)?d:[]);
  this.regFail=false;
  if(this.param.filename&&this.param.shaSrc!=='server')this.matchRegistry();
 }catch(e){this.reg=[];this.regFail=true}
 this.renderParam();
},
matchRegistry(){
 const fn=this.param.filename;
 let hit=this.reg.find(x=>String(x.filename||x.name||'')===fn);
 if(!hit&&this.param.version)hit=this.reg.find(x=>x.version===this.param.version&&(!this.param.arch||x.arch===this.param.arch));
 if(hit&&hit.sha256){
  this.param.sha256=String(hit.sha256).toLowerCase();
  this.param.shaSrc='registry';
  this.param.size=hit.size!=null?hit.size:null;
  return true;
 }
 return false;
},
renderExpiry(){
 const el=this.gid('upgExpiry');if(!el)return;
 if(!this.param.ok||!this.param.url){el.textContent='';el.className='pmeta';return}
 if(this.param.expiresAt==null){el.textContent='⚠ 未识别到有效期参数（Expires / x-oss-expires），请自行确认链接时效';el.className='pmeta warn';return}
 const rem=this.param.expiresAt-Date.now();
 const left=this.fmtRemain(rem);
 const until='至 '+new Date(this.param.expiresAt).toLocaleString('zh-CN',{hour12:false});
 if(rem<=0){el.textContent='链接已过期（'+until+'），请重新生成签名 URL 后粘贴';el.className='pmeta bad'}
 else if(rem<this.estMs()){el.textContent='有效期约剩 '+left+'（'+until+'）——低于预估批量时长 '+this.fmtRemain(this.estMs())+'（'+this.selected.size+' 台 · 6 并行），批量中途可能过期';el.className='pmeta warn'}
 else{el.textContent='有效期约剩 '+left+'（'+until+'）· 预估批量时长 '+this.fmtRemain(this.estMs());el.className='pmeta'}
},
renderParam(){
 const urlMeta=this.gid('upgUrlMeta'),shaMeta=this.gid('upgShaMeta'),srcEl=this.gid('upgShaSrc'),sumEl=this.gid('upgParamSummary'),regEl=this.gid('upgRegState');
 if(!urlMeta)return;
 if(this.regFail)regEl.textContent='包注册表接口不可用（后端未上线？），sha256 可手填';
 else regEl.textContent=this.reg.length?'注册表已加载 '+this.reg.length+' 条':'';
 if(!this.param.url){urlMeta.textContent='粘贴后自动识别文件名、版本与机型；签名 URL 属敏感凭据，请勿外传';urlMeta.className='pmeta'}
 else if(!this.param.ok){urlMeta.textContent='URL 格式无法解析，请检查后重新粘贴';urlMeta.className='pmeta bad'}
 else{
  urlMeta.textContent='文件名 '+(this.param.filename||'未识别')+' · 版本 '+(this.param.version||'未识别')+' · 机型 '+(this.param.arch||'未识别');
  urlMeta.className='pmeta'+(this.param.version&&this.param.arch?'':' warn');
 }
 const shaIn=this.gid('upgSha');
 if(this.param.shaSrc!=='manual'&&shaIn.value!==(this.param.sha256||''))shaIn.value=this.param.sha256||'';
 srcEl.textContent=this.param.shaSrc==='registry'?'注册表命中':this.param.shaSrc==='manual'?'手动填写':this.param.shaSrc==='server'?'服务端计算':'';
 srcEl.className='shaorigin '+(this.param.shaSrc||'');
 if(this.shaComputing){shaMeta.textContent='服务端计算中：正在从 OSS 直连拉取整包（约 1G，耗时视出口带宽），完成后自动填入并缓存进注册表…';shaMeta.className='pmeta'}
 else if(this.param.sha256||this.param.shaSrc==='manual'){shaMeta.textContent='';shaMeta.className='pmeta'}
 else if(this.param.ok&&this.param.filename){shaMeta.textContent=this.regFail?'注册表不可用且未填 sha256，可手填':'注册表未命中，可手填或点「服务端计算」';shaMeta.className='pmeta'}
 else{shaMeta.textContent='';shaMeta.className='pmeta'}
 this.renderExpiry();
 if(!this.param.ok||!this.param.filename)sumEl.innerHTML='';
 else sumEl.innerHTML='<span>目标版本 <b>'+esc(this.param.version||'—')+'</b></span><span>机型 <b>'+esc(this.param.arch||'—')+'</b></span>'+(this.param.size!=null?'<span>大小 <b>'+this.fmtB(this.param.size)+'</b></span>':'')+(this.param.sha256?'<span class=shalink>sha256 <span class=sha title="'+esc(this.param.sha256)+'">'+esc(this.param.sha256.slice(0,16))+'…</span><button type=button class=copymini data-sha="'+esc(this.param.sha256)+'">复制</button></span>':'');
 this.renderDevs();this.updateSelBar();this.updateActions();
},
async computeSha(){
 if(this.shaComputing)return;
 if(!this.param.ok||!this.param.url){this.setActHint('请先粘贴有效的 OSS 签名 URL',true);return}
 this.shaComputing=true;this.renderParam();
 try{
  const r=await fetch('/api/upgrade/packages/hash',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({oss_url:this.param.url})});
  const d=await r.json().catch(()=>({}));
  if(!r.ok){
   if(d.error_code==='oss_expired'){this.shaComputing=false;const m=this.gid('upgShaMeta');if(m){m.textContent='服务端计算失败：'+(d.error||'链接已过期')+'，请重新生成签名 URL';m.className='pmeta bad'}return}
   throw Error(d.error||('HTTP '+r.status));
  }
  if(d.status==='done'&&(d.entry||{}).sha256){
   this.param.sha256=String(d.entry.sha256).toLowerCase();this.param.shaSrc='server';
   if(d.entry.size!=null)this.param.size=d.entry.size;
   this.shaComputing=false;this.loadRegistry();
  }else this.startShaPoll();
 }catch(e){
  this.shaComputing=false;
  const m=this.gid('upgShaMeta');if(m){m.textContent='服务端计算失败：'+e.message+'（可手填）';m.className='pmeta bad'}
 }
},
regEntry(){return this.reg.find(x=>x.version===this.param.version&&x.arch===this.param.arch)||this.reg.find(x=>String(x.filename||'')===this.param.filename&&x.filename)},
async pollShaTick(){
 try{
  const r=await fetch('/api/upgrade/packages',{cache:'no-store'});
  const d=await r.json();
  if(!r.ok)return;
  this.reg=d.packages||d.items||(Array.isArray(d)?d:[]);
  this.regFail=false;
 }catch(e){return}
 const e=this.regEntry();
 if(e&&e.sha256){this.stopShaPoll();this.shaComputing=false;this.param.sha256=String(e.sha256).toLowerCase();this.param.shaSrc='server';if(e.size!=null)this.param.size=e.size;this.renderParam();return}
 if(e&&e.hash_status==='failed'){this.stopShaPoll();this.shaComputing=false;const m=this.gid('upgShaMeta');if(m){m.textContent='服务端计算失败：'+(e.error||e.error_code||'未知原因')+'（可手填）';m.className='pmeta bad'}return}
 if(e&&e.hash_status==='computing'){
  const m=this.gid('upgShaMeta');
  if(m){m.className='pmeta';m.textContent=e.total>0?('服务端计算中：已从 OSS 拉取 '+Math.min(100,Math.round((e.downloaded||0)/e.total*100))+'%（'+this.fmtB(e.downloaded||0)+' / '+this.fmtB(e.total)+'），完成后自动填入并缓存进注册表…'):'服务端计算中：正在从 OSS 直连拉取整包（约 1G，耗时视出口带宽），完成后自动填入并缓存进注册表…'}
 }
 const rs=this.gid('upgRegState');
 if(rs&&!this.regFail)rs.textContent=this.reg.length?('注册表已加载 '+this.reg.length+' 条'+(this.reg.some(x=>x.hash_status==='computing')?' · '+this.reg.filter(x=>x.hash_status==='computing').length+' 个计算中':'')):'';
},
startShaPoll(){
 this.stopShaPoll();this.shaPollN=0;
 this.pollShaTick();
 this.shaPollT=setInterval(()=>{
  this.shaPollN++;
  if(this.shaPollN>150){this.stopShaPoll();this.shaComputing=false;const m=this.gid('upgShaMeta');if(m){m.textContent='服务端计算超时（>10 分钟仍未完成），可稍后重试或手填';m.className='pmeta bad'}return}
  this.pollShaTick()},4000);
},
stopShaPoll(){if(this.shaPollT){clearInterval(this.shaPollT);this.shaPollT=null}},
isOssErr(t,code){
 if(String(code||'').toLowerCase()==='oss_expired')return true;
 t=String(t||'');
 if(!t)return false;
 return /(^|[^0-9])403([^0-9]|$)/i.test(t)||/access ?denied|oss_expired|forbidden/i.test(t)||/链接?过期|重新生成/.test(t);
},
devRows(){return this.fleet.filter(f=>!f.retired).map(f=>({h:f.hostname,site:f.site||'',online:!!f.online,ver:(f.version||'').trim()||'未获取',arch:this.archOf(f),hw:f.hardware||f.hw_model||''}))},
async loadFleet(){
 try{
  const r=await fetch('/api/hosts',{cache:'no-store'});const d=await r.json();
  if(!r.ok)throw Error('HTTP '+r.status);
  this.fleet=d.fleet||[];
  const online=this.fleet.filter(f=>!f.retired&&f.online).map(f=>f.hostname);
  if(!this.selTouched)this.selected=new Set(online);
  else{const live=new Set(online);for(const h of [...this.selected])if(!live.has(h))this.selected.delete(h)}
  this.renderDevs();this.updateSelBar();this.updateActions();
 }catch(e){if(!this.fleet.length){const el=this.gid('upgDevGroups');if(el)el.innerHTML='<p class=uempty>设备数据加载失败（/api/hosts 不可用），稍后自动重试</p>'};this.updateSelBar();this.updateActions()}
},
renderDevs(){
 const wrap=this.gid('upgDevGroups');
 const rows=this.devRows();
 if(!rows.length){wrap.innerHTML='<p class=uempty>暂无设备数据——设备总览接口不可用或未登记设备</p>';return}
 const groups={};
 for(const r of rows)(groups[r.ver]=groups[r.ver]||[]).push(r);
 const keys=Object.keys(groups).sort((a,b)=>a==='未获取'?1:b==='未获取'?-1:this.verCmp(b,a));
 let h='';
 for(const k of keys){
  const g=groups[k].sort((a,b)=>(b.online-a.online)||a.h.localeCompare(b.h,undefined,{numeric:true}));
  const on=g.filter(r=>r.online);const selN=on.filter(r=>this.selected.has(r.h)).length;
  h+=`<div class=vgroup><div class=vghead><input type=checkbox data-grp="${esc(k)}" ${on.length&&selN===on.length?'checked':''} ${on.length?'':'disabled'}><span class=vgt>${esc(k)}</span><span class=vgn>${on.length}/${g.length} 在线${selN?' · 已选 '+selN:''}</span><button type=button class=grpbtn data-grpup="${esc(k)}">全组升级</button></div><div class=vrows>`;
  for(const r of g){
   const tag=r.site?`<span class="stag vsite" style="--c:${siteColor(r.site)}">${esc(r.site)}</span>`:'';
   const mism=(this.param.arch&&r.arch&&r.arch!==this.param.arch)?'<span class=mism title="机型与目标包（'+esc(this.param.arch)+'）不符">机型不符</span>':'';
   h+=`<label class="vrow${r.online?'':' off'}" title="${r.online?'':'离线设备不可选'}"><input type=checkbox data-h="${esc(r.h)}" ${this.selected.has(r.h)?'checked':''} ${r.online?'':'disabled'}><span class=bh>${esc(r.h)}</span><small>${esc(r.arch||r.hw||'')}</small>${mism}${tag}</label>`}
  h+='</div></div>'
 }
 wrap.innerHTML=h;
},
updateSelBar(){
 const n=this.selected.size;const el=this.gid('upgSelBar');
 const vers=new Set();
 for(const h of this.selected){const f=this.fleet.find(x=>x.hostname===h);vers.add(((f&&f.version)||'').trim()||'未获取')}
 const cur=n?(vers.size===1?[...vers][0]:'多版本'):'—';
 el.innerHTML=`已选 <b>${n}</b> 台 · 当前版本 ${esc(cur)} → 目标版本 <b>${esc(this.targetVersion||'—')}</b>${n?' <button type=button class=minibtn data-clr>清空选择</button>':''}`;
 this.renderExpiry();
},
canLaunch(){return !this.active()&&this.param.ok&&!!this.param.version&&this.selected.size>0},
updateActions(){
 const busy=this.active();
 const noUrl=!this.param.ok||!this.param.url;
 const noVer=!this.param.version;
 const noSel=!this.selected.size;
 const expired=this.param.expiresAt!=null&&this.param.expiresAt-Date.now()<=0;
 const lowTime=this.param.expiresAt!=null&&!expired&&this.param.expiresAt-Date.now()<this.estMs();
 const noSha=!this.param.sha256;
 let mism=0;
 if(this.param.arch)for(const h of this.selected){const f=this.fleet.find(x=>x.hostname===h);const a=f&&this.archOf(f);if(a&&a!==this.param.arch)mism++}
 let hint='',warn=false;
 if(busy)hint='有升级任务进行中（#'+this.job.id+'），任务结束后才能发起新任务（后端并发护栏兜底）';
 else if(expired)hint='OSS 链接已过期，请重新生成签名 URL 后粘贴';
 else if(noUrl)hint='请先在「① 升级参数」粘贴 OSS 签名 URL（自动识别版本/机型）';
 else if(noVer)hint='URL 中未识别出版本号（文件名需形如 ego-portal-v0.4.6-rk3588-arm64.tar.gz）',warn=true;
 else if(noSel)hint='请至少勾选一台在线设备（默认已勾选全部在线设备，离线设备不可选）';
 else if(mism)hint='⚠ 所选设备中 '+mism+' 台机型与目标包（'+this.param.arch+'）不符，下载/安装类操作不可发起；请改选相符机型设备，或更换对应机型的包链接',warn=true;
 else if(noSha)hint='⚠ 尚无 sha256 基准：下载/安装类操作不可发起（注册表未命中时请手填或点「服务端计算」）；仅预检/验证/清理可发起',warn=true;
 else if(lowTime)hint='⚠ 链接剩余有效期低于预估批量时长（'+this.fmtRemain(this.estMs())+'），批量中途可能过期，建议重新生成更长时效链接',warn=true;
 if(busy&&this.stickyNote&&Date.now()<this.stickyNote.until){hint=this.stickyNote.text;warn=this.stickyNote.warn}
 else if(!busy)this.stickyNote=null;
 const heavy=busy||noUrl||noVer||noSel||expired||mism>0||noSha;
 const light=busy||noUrl||noVer||noSel||expired;
 this.gid('upgRunAll').disabled=heavy;
 document.querySelectorAll('.stagebtn[data-kind]').forEach(b=>{const k=b.dataset.kind;b.disabled=(k==='download'||k==='install')?heavy:light});
 this.setActHint(hint,warn);
},
closeConfirm(){const m=this.gid('upgModal');if(m)m.classList.remove('show')},
showConfirm(){
 const n=this.selected.size;const vs=this.targetVersion;
 const archSplit={};let mism=0;
 for(const h of this.selected){const f=this.fleet.find(x=>x.hostname===h);const a=(f&&this.archOf(f))||'未知机型';if(this.param.arch&&a!=='未知机型'&&a!==this.param.arch){mism++;archSplit['机型不符']=(archSplit['机型不符']||0)+1}else archSplit[a]=(archSplit[a]||0)+1}
 const archTxt=Object.entries(archSplit).map(([a,c])=>esc(a)+' × '+c).join('、');
 const list=[...this.selected].sort((a,b)=>a.localeCompare(b,undefined,{numeric:true}));
 const preview=esc(list.length>12?list.slice(0,12).join('、')+' 等 '+n+' 台':list.join('、'));
 const remTxt=this.param.expiresAt==null?'未识别（请自行确认链接时效）':((this.param.expiresAt-Date.now()>0?'约剩 ':'')+this.fmtRemain(this.param.expiresAt-Date.now())+(this.param.expiresAt-Date.now()>0?'（至 '+new Date(this.param.expiresAt).toLocaleString('zh-CN',{hour12:false})+'）':''));
 const m=this.gid('upgModalBody');
 m.innerHTML=`<header><div><span class=name>确认发起全流程升级</span></div><button type=button class="x" id=upgConfirmClose>✕</button></header>
 <div class=mgrid>
  <span>目标版本</span><b>${esc(vs)}（${esc(this.param.arch||'机型未识别')} · ${esc(this.param.filename||'文件名未识别')}）</b>
  <span>设备数量</span><b>${n} 台（${archTxt}${mism?'，其中 '+mism+' 台机型不符':''}）</b>
  <span>设备清单</span><b>${preview}</b>
  <span>链接有效期</span><b>${esc(remTxt)}</b>
  <span>执行流程</span><b>预检 → 下载 → 准备 → 安装 → 验证 → 清理（设备直连 OSS 下载，sha256 基准校验）</b>
 </div>
 <p class=uwarn>⚠ 升级将覆盖设备上的程序，过程不可逆；单台失败不影响其他设备，安装器失败时会自动回滚兜底。请确认目标版本、机型与设备清单无误后再发起。</p>
 <div class=ubtns style="justify-content:flex-end;margin-top:16px"><button type=button class=stagebtn id=upgConfirmNo>取消</button><button type=button id=upgConfirmYes>确认发起升级</button></div>`;
 this.gid('upgModal').classList.add('show');
 this.gid('upgConfirmClose').onclick=this.gid('upgConfirmNo').onclick=()=>this.closeConfirm();
 this.gid('upgConfirmYes').onclick=()=>{this.closeConfirm();this.launch('upgrade')};
},
async launch(kind){
 const devices=[...this.selected];
 this.setActHint('正在创建任务…');
 try{
  const r=await fetch('/api/upgrade/jobs',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({kind,devices,params:{version:this.targetVersion,oss_url:this.param.url,sha256:this.param.sha256||undefined}})});
  const d=await r.json().catch(()=>({}));
  if(!r.ok){
   if(d.error_code==='oss_expired'){this.param.expiresAt=Date.now()-1000;this.renderParam();throw Error((d.error||'OSS 链接已过期')+'（后端校验拒绝）')}
   if(d.error_code==='sha_missing')throw Error(d.error||'缺少 sha256 基准');
   throw Error(d.error||('HTTP '+r.status));
  }
  const j=d.job||d;const id=j.id!=null?j.id:j.job_id;
  if(id==null)throw Error('响应缺少任务 id（job_id）');
  if(d.expires_at!=null){const t=this.parseTs(d.expires_at);if(t)this.param.expiresAt=t}
  this.fromSnap={};for(const h of devices){const f=this.fleet.find(x=>x.hostname===h);if(f)this.fromSnap[h]=f.version||''}
  const effective=(Array.isArray(d.devices)&&d.devices.length)?d.devices:devices;
  this.adoptJob({id:id,kind:j.kind||kind,version:j.version||this.targetVersion,filename:j.filename||this.param.filename||'',status:j.status||'running',created_at:j.created_at||new Date().toISOString(),finished_at:j.finished_at||null,devices:effective});
  let note='任务 #'+id+' 已创建';
  if(Array.isArray(d.skipped)&&d.skipped.length)note+='；'+d.skipped.length+' 台被跳过：'+d.skipped.map(s=>esc(s.hostname)+'（'+esc(s.reason||s.error||'原因未说明')+'）').join('、');
  if(Array.isArray(d.warnings)&&d.warnings.length)note+='；⚠ '+d.warnings.map(w=>esc(typeof w==='string'?w:(w.message||w.msg||''))).join('；');
  this.stickyNote={text:note,warn:!!(d.warnings&&d.warnings.length),until:Date.now()+20000};
  this.setActHint(this.stickyNote.text,this.stickyNote.warn);
  this.loadJobDetail(id);this.subscribe();
 }catch(e){this.setActHint('发起失败：'+e.message,true)}
},
normJob(j){return{id:j.id!=null?j.id:j.job_id,kind:j.kind||'',version:j.version||(j.params&&j.params.version)||'',filename:j.filename||'',oss_url_expires:j.oss_url_expires!=null?j.oss_url_expires:null,error:j.error||j.error_code||'',error_code:j.error_code||'',status:j.status||'running',created_at:j.created_at||j.createdAt||null,finished_at:j.finished_at||j.finishedAt||null,devices:j.devices||[],summary:j.summary||null}},
normTask(t){return{hostname:t.hostname||t.host||'',stage:String(t.stage||'').toLowerCase(),status:this.normStatus(t.status),error:t.error||t.reason||'',error_code:t.error_code||'',started_at:t.started_at||t.startedAt||null,finished_at:t.finished_at||t.finishedAt||null,from_version:t.from_version||t.version_before||''}},
adoptJob(j){this.job=j;this.tasks=(j.devices||[]).map(h=>({hostname:h,stage:'',status:'pending',error:'',from_version:this.fromSnap[h]||''}));this.logs={};this.logOff={};this.expanded=null;this.renderJob()},
async loadJobs(){
 try{
  const r=await fetch('/api/upgrade/jobs',{cache:'no-store'});const d=await r.json();
  if(!r.ok)throw Error(d.error||('HTTP '+r.status));
  const list=d.jobs||d.items||(Array.isArray(d)?d:[]);
  if(!list.length){if(!this.job)this.renderJob();return}
  const j=this.normJob(list[0]);
  if(j.id==null)return;
  if(!this.job||this.job.id!==j.id){this.adoptJob(j);this.loadJobDetail(j.id)}
  else{this.job=j;this.renderJob()}
  if(this.active())this.subscribe();
 }catch(e){if(!this.job){const el=this.gid('upgJobSummary');if(el)el.innerHTML='<p class=uempty>升级任务接口暂不可用（后端未上线？），发起任务后此处展示逐台进度</p>';this.gid('upgTaskGrid').innerHTML=''}}
},
async loadJobDetail(id){
 try{
  const r=await fetch('/api/upgrade/jobs/'+encodeURIComponent(id),{cache:'no-store'});const d=await r.json();
  if(!r.ok)throw Error(d.error||('HTTP '+r.status));
  const j=this.normJob(d.job||d);
  if(this.job&&this.job.id!==j.id)return;
  this.job=j;
  const ts=(d.tasks||j.tasks||[]).map(t=>this.normTask(t));
  if(ts.length)this.tasks=ts;
  else if((j.devices||[]).length&&!this.tasks.length)this.tasks=j.devices.map(h=>({hostname:h,stage:'',status:'pending',error:'',from_version:''}));
  this.renderJob();
  if(!this.active())this.teardown();
 }catch(e){/*保留已有状态，等待下次刷新/SSE*/}
},
renderJob(){
 this.updateActions();
 const sumEl=this.gid('upgJobSummary'),grid=this.gid('upgTaskGrid');
 if(!this.job){sumEl.innerHTML='<p class=uempty>暂无升级任务——在上方勾选设备发起后，这里展示逐台进度与日志</p>';grid.innerHTML='';return}
 const j=this.job;const st=this.normStatus(j.status);
 const cnt={success:0,failed:0,blocked:0,running:0,pending:0};
 for(const t of this.tasks)cnt[t.status]=(cnt[t.status]||0)+1;
 const total=this.tasks.length||((j.devices||[]).length)||((j.summary&&j.summary.total)||0);
 const created=this.parseTs(j.created_at);const fin=this.parseTs(j.finished_at);
 const end=fin||(st==='running'?Date.now():null);
 const el=(created&&end)?(end-created)/1000:null;
 let oss=false;
 if(this.isOssErr(j.error,j.error_code))oss=true;
 if(!oss)for(const t of this.tasks){if(t.status==='failed'&&this.isOssErr(t.error,t.error_code)){oss=true;break}}
 sumEl.innerHTML=(oss?'<div class=ossban>⚠ OSS 链接已过期（403），设备下载被拒——请重新生成签名 URL 并新建任务</div>':'')+`<b>#${esc(j.id)}</b><span>${esc(this.KIND_LBL[j.kind]||j.kind||'任务')} → <b>${esc(j.version||'—')}</b></span><span class="tstat ${st}">${this.STAT_LBL[st]||esc(st)}</span><span>共 <b>${total}</b> 台</span><span>成功 <b style="color:#15803d">${cnt.success}</b></span><span>失败 <b style="color:#b91c1c">${cnt.failed}</b></span><span>阻断 <b style="color:#b45309">${cnt.blocked}</b></span><span>进行中 <b style="color:#1d4ed8">${cnt.running}</b></span><span>等待 <b>${cnt.pending}</b></span>${el!=null?`<span>耗时 <b>${this.fmtDur2(el)}</b></span>`:''}${created?`<span>发起于 ${esc(this.fmtClock(created))}</span>`:''}${j.oss_url_expires?`<span>链接有效期 ${esc(this.fmtRemain(j.oss_url_expires*1000-Date.now()))}</span>`:''}`;
 grid.innerHTML=this.tasks.length?this.tasks.map(t=>this.cardHtml(t)).join(''):(this.active()?'<p class=uempty>任务已创建，等待设备状态上报…</p>':'');
},
cardHtml(t){
 const h=t.hostname;const f=this.fleet.find(x=>x.hostname===h);
 const site=(f&&f.site)||'';const tag=site?`<span class="stag" style="--c:${siteColor(site)}">${esc(site)}</span>`:'';
 const from=t.from_version||this.fromSnap[h]||((f&&f.version)||'');
 const to=this.job.version||this.targetVersion||'';
 const st=t.status;
 const cls=st==='success'?'done':st==='failed'?'failed':st==='blocked'?'blocked':'';
 const scopeIdx=this.job.kind==='upgrade'?null:this.STAGES.indexOf(this.job.kind);
 const curIdx=this.STAGES.indexOf(t.stage);
 let steps='';
 for(let i=0;i<this.STAGES.length;i++){
  let c='';
  if(scopeIdx!=null&&i!==scopeIdx)c='na';
  else if(st==='success')c='done';
  else if(st==='failed'||st==='blocked')c=i===curIdx?st:(curIdx>=0&&i<curIdx?'done':'');
  else if(st==='running')c=i===curIdx?'cur':(curIdx>=0&&i<curIdx?'done':'');
  steps+=`<span class="step ${c}">${this.STAGE_LBL[this.STAGES[i]]}</span>`
 }
 const errBox=(st==='failed'||st==='blocked')&&t.error?`<div class="terr ${st}">${esc(st==='blocked'?'阻断：':'失败：')}${esc(t.error)}</div>`:'';
 return `<article class="ucard ${cls}${this.expanded===h?' expanded':''}" data-h="${esc(h)}">
 <div class=uh><span class=name>${esc(h)}</span>${tag}<span class="tstat ${st}">${this.STAT_LBL[st]||esc(st)}</span></div>
 <div class=uver>版本：${esc(from||'—')} → <b>${esc(to||'—')}</b></div>
 <div class=stepper>${steps}</div>
 ${errBox}
 <div class=ulogwrap><div class=ulogbar><span>实时日志（SSE 增量，点击卡片收起）</span><button type=button data-fl="${esc(h)}">${this.follow[h]===false?'恢复跟随':'暂停跟随'}</button></div><pre class=ulog>${esc(this.logs[h]||'')}</pre></div>
 </article>`;
},
logDom(h){try{return document.querySelector('.ucard[data-h="'+(window.CSS&&CSS.escape?CSS.escape(h):h)+'"] .ulog')}catch(e){return null}},
toggleLog(h){
 this.expanded=this.expanded===h?null:h;
 this.renderJob();
 if(this.expanded===h&&this.job)this.fetchLog(h);
},
appendLog(h,txt){
 if(h==null||txt==null)return;let s=String(txt);if(!s)return;
 if(!this.logs[h])this.logs[h]='';
 if(!s.endsWith('\n'))s+='\n';
 this.logs[h]+=s;
 if(this.expanded===h){const pre=this.logDom(h);if(pre){pre.textContent=this.logs[h];if(this.follow[h]!==false)pre.scrollTop=pre.scrollHeight}}
},
async fetchLog(h){
 if(!this.job||this.job.id==null)return;
 const off=this.logOff[h]||0;
 try{
  const r=await fetch('/api/upgrade/jobs/'+encodeURIComponent(this.job.id)+'/log?offset='+off+'&hostname='+encodeURIComponent(h),{cache:'no-store'});
  const d=await r.json();
  if(!r.ok)return;
  let txt=d.text||d.chunk||d.data||(Array.isArray(d.lines)?d.lines.join('\n'):'');
  if(!txt)return;
  if(d.offset!=null)this.logOff[h]=d.offset;else this.logOff[h]=off+String(txt).length;
  this.appendLog(h,txt);
 }catch(e){}
},
setConn(s){this.connState=s;const el=this.gid('upgConn');if(!el)return;el.className='connchip '+s;el.textContent=s==='live'?'● 实时连接正常':s==='poll'?'○ 实时连接断开 · 轮询中':'未连接'},
subscribe(){
 if(!this.active())return;
 if(this.es&&this.es.readyState<=1)return;
 try{this.es=new EventSource('/api/upgrade/events')}catch(e){this.degrade();return}
 this.es.onopen=()=>{this.setConn('live');if(this.pollT){clearInterval(this.pollT);this.pollT=null}};
 this.es.onmessage=e=>this.onEvent('message',e.data);
 for(const n of['job','task','log','stage','progress'])this.es.addEventListener(n,e=>this.onEvent(n,e.data));
 this.es.onerror=()=>this.degrade();
},
degrade(){this.setConn('poll');if(!this.pollT&&(this.active()||this.expanded!=null))this.pollT=setInterval(()=>this.pollTick(),5000)},
pollTick(){if(!this.job)return;this.loadJobDetail(this.job.id);if(this.expanded!=null)this.fetchLog(this.expanded)},
teardown(){if(this.es){this.es.close();this.es=null}if(this.pollT){clearInterval(this.pollT);this.pollT=null}this.setConn('off')},
onEvent(name,raw){
 let m=null;const s=String(raw==null?'':raw);
 if(s.trim().startsWith('{')){try{m=JSON.parse(s)}catch(e){m=null}}
 if(!m)return;
 const type=String(m.type||name||'').toLowerCase();
 if(type==='webfix'){loadWebfix();return}   // 补配任务事件（KVWO-19 共流）：只驱动总览刷新，不进升级视图
 if(type==='webfix_log')return;             // 补配日志增量：升级日志面板不消费
 if(type==='job'||m.job){
  const j=this.normJob(m.job||m);
  if(j.id==null)return;
  if(!this.job||this.job.id!==j.id)this.adoptJob(j);else{this.job=j}
  this.renderJob();
  if(!this.active()){this.teardown();this.loadJobDetail(j.id)}
  return}
 if(type==='pkg'){
  if(this.shaComputing&&m.version!=null&&String(m.version)===String(this.param.version)&&(!m.arch||String(m.arch)===String(this.param.arch))){
   if(m.status==='ok'&&m.sha256){this.stopShaPoll();this.shaComputing=false;this.param.sha256=String(m.sha256).toLowerCase();this.param.shaSrc='server';if(m.size!=null)this.param.size=m.size;this.renderParam()}
   else this.pollShaTick();
  }else if(m.status==='ok')this.loadRegistry();
  return}
 if(type==='task'||m.task){this.applyTask(this.normTask(m.task||m));return}
 if(type==='log'||m.text||m.chunk||m.lines){
  const h=m.hostname;if(h!=null){let txt=m.text||m.chunk||(Array.isArray(m.lines)?m.lines.join('\n'):'');if(m.offset!=null)this.logOff[h]=m.offset;this.appendLog(h,txt)}
  return}
 if(m.hostname&&(m.stage!=null||m.status!=null))this.applyTask(this.normTask(m));
},
applyTask(t){
 if(!t.hostname)return;
 if(!this.job||!this.tasks.length)this.tasks.push(t);
 const i=this.tasks.findIndex(x=>x.hostname===t.hostname);
 if(i>=0)this.tasks[i]=Object.assign({},this.tasks[i],t);else this.tasks.push(t);
 this.renderJob();
}
};
/* ================= 资产管理视图（Assets.*，KVWO-84：总览仪表盘/硬件台账 360°/项目/版本） =================
 契约对齐后端 KVWO-83（/api/assets 系列，KVWO-97 库存口径修正为上海+潍坊；第六波 KVWO-107：闲置 threshold_days 默认 15）：
   GET /api/assets?status=&type=&unit=&q=&archived=1 → {devices:[…],last_sync_at}
   GET /api/assets/stats → {registered_total,archived_total,inventory:{total,by_unit,units,unused_note},
                            deployed,by_status,by_type,by_unit,repair_rate,damage_rate,last_sync_at}
   GET /api/assets/units → {units:[{unit,total,normal,normal_rate,online,usage_rate,
                            repairing,abnormal,last_updated}]}（KVWO-107：online=frps 在线
                            快照数，usage_rate=online/total，在册 0 记 null）
   GET /api/assets/versions → {detected:[{version,count}],ledger:[…],ledger_empty,matched,
                              detected_only:[…],ledger_missing:[…],
                              by_provider_top5:[{unit,total,versions:[{version,count,share}]}]}
                              （KVWO-107：在册数前 5 单位，探测版本分桶，未探测归「未获取」，
                              share=count/在册数两位小数）
   GET /api/assets/{device_code} → {ledger:{…},repairs:[…],flows:[…],parts:[…],transfers:[…]}
 接口未合并（404）时按契约 mock 并以横幅明示（绝不静默冒充生产数据）；后端合入后 404 消失
 即自动切真实数据，前端零改动。字段解析容错（候选键），联调以真实字段为准。
 导航：顶栏「资产」页签 + 子页签（总览/硬件/维修/版本/项目）。「维修看板」槽位由 KVWO-80 的 Rep.*
 填充（KVWO-88 并入），#view=repairs 深链兼容路由到本子页签。 */
const Assets={
mock:false,err:'',lastGoodAt:0,loaded:false,tab:'dashboard',drillUnit:'',
stats:null,units:null,versions:null,devices:[],archived:[],_mockCache:null,
hw:{q:'',status:'',type:'',unit:'',archived:false,sortKey:'code',sortDir:1},
STATUS_ORDER:['正常','异常','维修中','待维修','待标定','测试中','已拆解'],
ST_COLOR:{'正常':'#16a34a','异常':'#dc2626','维修中':'#ea580c','待维修':'#d97706','待标定':'#2563eb','测试中':'#0891b2','已拆解':'#64748b'},
TYPE_COLOR:{'EgoPortal-Stereo-V0':'#2563eb','EgoPortal-Stereo-V1':'#7c3aed','EgoPortal-Lite':'#0891b2'},
init(){
 const tabs=document.getElementById('assetTabs');
 tabs.addEventListener('click',e=>{const b=e.target.closest('button[data-atab]');if(b)this.go(b.dataset.atab)});
 window.addEventListener('hashchange',()=>{if(!document.getElementById('viewAssets').hidden)this.applyHash()});
 document.getElementById('asQ').oninput=e=>{this.hw.q=e.target.value.trim().toLowerCase();this.renderLedger()};
 document.getElementById('asStatus').onchange=e=>{this.hw.status=e.target.value;this.renderLedger()};
 document.getElementById('asType').onchange=e=>{this.hw.type=e.target.value;this.renderLedger()};
 document.getElementById('asUnit').onchange=e=>{this.hw.unit=e.target.value;this.renderLedger()};
 document.getElementById('asArchived').onchange=e=>{this.hw.archived=e.target.checked;this.renderLedger()};
 document.getElementById('asReportPush').onclick=()=>this.pushReport();
 document.getElementById('asLedgerHead').onclick=e=>{const th=e.target.closest('th[data-sk]');if(!th)return;if(this.hw.sortKey===th.dataset.sk)this.hw.sortDir*=-1;else{this.hw.sortKey=th.dataset.sk;this.hw.sortDir=1}this.renderLedger()};
 document.getElementById('asLedgerBody').onclick=e=>{this.rowClick(e)};
 document.getElementById('asUnitDetail').addEventListener('click',e=>{
  if(e.target.closest('#asUnitBack')){this.drillUnit='';document.getElementById('asUnitDetail').hidden=true;document.getElementById('asUnitBody').hidden=false;this.writeHash({atab:'units'});return}
  this.rowClick(e)});
 setInterval(()=>{if(!document.getElementById('viewAssets').hidden&&this.loaded&&!this.mock)this.loadAll()},60000);
 if(!document.getElementById('viewAssets').hidden)this.enter();
},
rowClick(e){
 const rb=e.target.closest('button[data-gorep]');
 if(rb){this.go('repairs',{q:rb.dataset.gorep});return}
 const tr=e.target.closest('tr[data-code]');if(tr)this.open360(tr.dataset.code)
},
enter(){this.applyHash();if(!this.loaded)this.loadAll();else this.syncBar()},
applyHash(){
 const p=new URLSearchParams(location.hash.slice(1));
 const vh=p.get('view');if(vh!=='assets'&&vh!=='repairs')return;   // view=repairs → 维修看板深链兼容（KVWO-88）
 const t=p.get('atab')||(vh==='repairs'?'repairs':'dashboard');
 this.setTab(['dashboard','hardware','repairs','versions','units'].includes(t)?t:'dashboard');
 if(p.get('q')!=null){this.hw.q=String(p.get('q')).toLowerCase();const el=document.getElementById('asQ');if(el)el.value=p.get('q')||''}
 const u=p.get('unit');if(u&&this.units)this.drill(u);
 if(this.loaded)this.render();
},
writeHash(extra){
 const p=new URLSearchParams(location.hash.slice(1));p.set('view','assets');
 for(const k of['atab','q','unit'])p.delete(k);
 if(extra)for(const kv of Object.entries(extra))p.set(kv[0],kv[1]);
 history.replaceState(null,'',location.pathname+location.search+'#'+p.toString())
},
go(tab,extra){this.setTab(tab);this.writeHash(Object.assign({atab:tab},extra||{}));if(tab==='repairs')this.renderRepairs()},
setTab(t){
 this.tab=t;
 if(t!=='units')this.drillUnit='';   // 离开项目页签即退出下钻态，避免深链/切页被旧下钻拉回
 document.querySelectorAll('#assetTabs button').forEach(b=>b.classList.toggle('on',b.dataset.atab===t));
 for(const s of['dashboard','hardware','repairs','versions','units'])document.getElementById('atab'+s[0].toUpperCase()+s.slice(1)).hidden=s!==t;
 if(t==='units'&&!this.drillUnit){document.getElementById('asUnitDetail').hidden=true;document.getElementById('asUnitBody').hidden=false}
},
/* ---------- 数据层：真实 API 优先，404 时契约 mock（横幅明示） ---------- */
async api(path){
 const r=await fetch(path,{cache:'no-store'});
 if(r.status===404){this.mock=true;return this.mockFor(path)}
 const d=await r.json().catch(()=>null);
 if(!r.ok||d==null)throw Error((d&&d.error)||('HTTP '+r.status));
 this.mock=false;return d
},
async loadAll(){
 try{
  const res=await Promise.all([this.api('/api/assets/stats'),this.api('/api/assets/units'),this.api('/api/assets/versions'),this.api('/api/assets'),this.api('/api/assets?archived=1')]);
  this.stats=this.normStats(res[0]);this.units=this.normUnits(res[1]);this.versions=this.normVers(res[2]);
  this.devices=this.normList(res[3],false);this.archived=this.normList(res[4],true);
  this.err='';this.loaded=true;this.lastGoodAt=Date.now();
 }catch(e){this.err=String(e.message||e)}
 this.syncBar();this.render();
},
normList(d,archived){const arr=(d&&(d.devices||d.items||d.records))||(Array.isArray(d)?d:[]);return arr.map(r=>this.normDev(r,archived)).filter(x=>x.code)},
nByKey(o,ks){for(const k of ks)if(o&&o[k]!=null)return o[k];return null},
/* 闲置阈值天数：文案一律取后端 stats.idle.threshold_days（KVWO-107 起默认 15），无返回才兑底 15，不写死 */
idleDays(){const d=this.stats&&this.stats.idleDays;return d!=null?d:15},
normDev(r,archived){
 const on=r?(r.device_online!==undefined?r.device_online:r.online):null;
 return{code:String(this.nByKey(r,['device_code','device_id','code'])||'').trim(),
  type:this.nByKey(r,['device_type','type']),status:this.nByKey(r,['status']),
  unit:this.nByKey(r,['unit','organization']),parts:this.nByKey(r,['accessories','parts']),
  remark:this.nByKey(r,['remark','note']),ledgerVer:this.nByKey(r,['ledger_version','software_version']),
  detVer:this.nByKey(r,['detected_version']),online:on===true?true:(on===false?false:null),
  openRepairs:Number(this.nByKey(r,['open_repairs','repair_count'])||0),
  updatedAt:this.nByKey(r,['updated_at','last_updated']),modifier:this.nByKey(r,['modifier']),
  lastSeen:this.nByKey(r,['last_seen','lastSeen']),idle:this.nByKey(r,['idle']),
  archived:!!(archived||(r&&r.archived))}
},
normStats(s){return{
 total:this.nByKey(s,['registered_total','total','registered']),archived:this.nByKey(s,['archived_total','archived']),
 invTotal:this.nByKey(s&&s.inventory,['total']),invB:this.nByKey(s&&s.inventory,['unused_note']),
 invUnits:(s&&s.inventory&&Array.isArray(s.inventory.units))?s.inventory.units:[],
 invByUnit:Object.assign({},(s&&s.inventory&&s.inventory.by_unit)||{}),
 idleTotal:this.nByKey(s&&s.idle,['total']),idleDays:this.nByKey(s&&s.idle,['threshold_days']),
 idleByUnit:Object.assign({},(s&&s.idle&&s.idle.by_unit)||{}),
 deployed:this.nByKey(s,['deployed']),
 byStatus:Object.assign({},s&&(s.by_status||s.status)||{}),byType:Object.assign({},s&&(s.by_type||s.type)||{}),byUnit:Object.assign({},s&&(s.by_unit||s.unit)||{}),
 repairRate:this.nByKey(s,['repair_rate']),damageRate:this.nByKey(s,['damage_rate']),
 lastSync:this.nByKey(s,['last_sync_at','last_sync','synced_at'])}
},
normUnits(u){const arr=(u&&(u.units||u.items))||(Array.isArray(u)?u:[]);return arr.map(x=>({
 unit:String(this.nByKey(x,['unit','name'])||''),total:this.nByKey(x,['total','count']),
 normalRate:this.nByKey(x,['normal_rate']),repairing:this.nByKey(x,['repairing']),abnormal:this.nByKey(x,['abnormal']),
 online:this.nByKey(x,['online','online_count']),usageRate:this.nByKey(x,['usage_rate']),   // 第六波 KVWO-107：在线数与使用率（在线/在册，null → 「—」）
 lastUpdated:this.nByKey(x,['last_updated','updated_at'])}))
},
normVers(v){const arr=a=>Array.isArray(a)?a:[];return{
 detected:arr(v&&v.detected),ledger:arr(v&&v.ledger),
 ledgerEmpty:this.nByKey(v,['ledger_empty']),matched:this.nByKey(v,['matched']),
 detectedOnly:arr(v&&v.detected_only),ledgerMissing:arr(v&&v.ledger_missing),
 byProviderTop5:arr(v&&v.by_provider_top5).map(p=>({unit:String(this.nByKey(p,['unit','name'])||''),total:this.nByKey(p,['total','count']),
  versions:arr(p&&p.versions).map(x=>({version:this.nByKey(x,['version']),count:this.nByKey(x,['count']),share:this.nByKey(x,['share'])}))}))}   // 第六波 KVWO-107：主要服务商版本占比（在册数前 5）
},
fmtTs(v){
 if(v==null||v==='')return null;
 const t=typeof v==='number'?(v<1e12?v*1000:v):Date.parse(String(v).replace(' ','T'));
 return isNaN(t)?String(v):new Date(t).toLocaleString('zh-CN',{hour12:false})
},
tsOf(v){if(v==null||v==='')return 0;const t=typeof v==='number'?(v<1e12?v*1000:v):Date.parse(String(v).replace(' ','T'));return isNaN(t)?0:t},
shortType(t){return String(t||'').replace('EgoPortal-','')},
natCmp(a,b){return String(a).localeCompare(String(b),undefined,{numeric:true,sensitivity:'base'})},
syncBar(){
 const el=document.getElementById('asSyncBar');el.hidden=false;
 const when=this.stats&&this.stats.lastSync?('飞书同步于 '+this.fmtTs(this.stats.lastSync)):'';
 const good=this.lastGoodAt?('拉取于 '+new Date(this.lastGoodAt).toLocaleString('zh-CN',{hour12:false})):'';
 if(this.mock){el.className='asyncbar warn';el.innerHTML='⚠ /api/assets 接口未就绪（后端 KVWO-83 未合并）：当前展示<b>契约 mock 演示数据</b>，仅供开发预览，不代表真实资产；后端合入后自动切换真实数据，前端无需改动'}
 else if(this.err&&!this.loaded){el.className='asyncbar err';el.innerHTML='资产数据加载失败：'+esc(this.err)+'（每 60 秒自动重试）'}
 else if(this.err){el.className='asyncbar err';el.innerHTML='最近一次刷新失败：'+esc(this.err)+'，展示最后成功数据'+(good?'（'+good+'）':'')+(when?' · '+when:'')}
 else{el.className='asyncbar ok';el.innerHTML='资产数据'+(when||'')+(good?' · '+good:'')+' · 来源：飞书多维表（只读同步）'}
},
render(){if(!this.loaded)return;this.renderMetrics();this.renderDists();this.renderFilters();this.renderLedger();this.renderRepairs();this.renderVersions();this.renderUnits()},
invLabel(){
 const st=this.stats||{},bu=st.invByUnit||{};const parts=(st.invUnits&&st.invUnits.length?st.invUnits:['上海办公室','潍坊办公室']).map(u=>u+' '+((bu[u]!=null?bu[u]:0))+' 台');
 return (parts.length?parts.join(' + '):'—')+'（公司库存，服务商持有不计）'
},
/* ---------- 日报手动推送（测试按钮，KVWO-97 评论区要求）：POST /api/assets/report/run，成功弹窗展示消息全文 ---------- */
async pushReport(){
 const btn=document.getElementById('asReportPush'),st=document.getElementById('asReportState');
 if(!btn||btn.disabled)return;
 btn.disabled=true;st.textContent='推送中…';
 try{
  const r=await fetch('/api/assets/report/run',{method:'POST',cache:'no-store'});
  const d=await r.json().catch(()=>null);
  if(!r.ok||!d||!d.ok){st.textContent='推送失败：'+((d&&d.error)||('HTTP '+r.status));return}
  st.textContent='已推送（'+(d.reason||'manual')+' · '+(this.fmtTs((d.at||0)*1000)||'')+'），消息全文见弹窗';
  const m=document.getElementById('modal');
  document.getElementById('modalBody').innerHTML='<header><div><span class=name>资产日报 · 推送成功（飞书群应已收到）</span></div><button class=x id=mClose>✕</button></header><pre class=rptext>'+esc(d.text||'')+'</pre>';
  m.classList.add('show');
  document.getElementById('mClose').onclick=()=>m.classList.remove('show');
 }catch(e){st.textContent='推送失败：'+String(e.message||e)}
 finally{btn.disabled=false}
},
/* ---------- 模块 1：总览仪表盘 ---------- */
renderMetrics(){
 const s=this.stats,el=document.getElementById('asMetrics');if(!s){el.innerHTML='';return}
 const n=k=>Number((s.byStatus||{})[k]||0);
 const pctv=v=>v==null?'—':(Math.round(v*1000)/10)+'%';
 const st='统计时间：'+(this.fmtTs(s.lastSync)||'—');
 const cards=[
  {v:s.total,l:'在册总数',sub:'口径：设备明细表（不含归档）'},
  {v:s.archived,l:'归档（已拆解）',sub:'口径：设备归档数据表，不参与在册指标'},
  {v:s.invTotal,l:'库存',sub:'口径：'+this.invLabel()+(s.invB!=null?'；参考：备注含「未使用」'+s.invB+' 台':''),c:'#0f766e'},
  {v:s.deployed,l:'一线部署',sub:'口径：在册 − 库存（库存单位持有不计部署）'},
  {v:pctv(s.repairRate),l:'维修率',rate:1,c:'#ea580c',sub:'计算式：(维修中 '+n('维修中')+' + 待维修 '+n('待维修')+') / 在册 '+s.total},
  {v:pctv(s.damageRate),l:'损坏率',rate:1,c:'#dc2626',sub:'计算式：(异常 '+n('异常')+' + 维修中 '+n('维修中')+' + 待维修 '+n('待维修')+') / 在册 '+s.total},
  {v:s.idleTotal,l:'闲置',sub:'口径：库存外·超 '+this.idleDays()+' 天未上线（含从未上线）；fleet 库未启用时不可计算',c:'#7c3aed'},
 ];
 el.innerHTML=cards.map(m=>'<div class="amcard'+(m.rate?' rate':'')+'" style="--c:'+(m.c||'')+'" title="'+esc(m.sub)+'\n'+esc(st)+'"><b>'+esc(m.v==null?'—':m.v)+'</b><span class=albl>'+esc(m.l)+'</span><div class=asub2>'+esc(m.sub)+'<br>'+esc(st)+'</div></div>').join('');
},
renderDists(){
 const el=document.getElementById('asDists'),s=this.stats;if(!s){el.innerHTML='';return}
 const bars=(entries,colorFn,labelFn)=>{
  const max=Math.max.apply(null,entries.map(e=>e[1]).concat([1]));
  return entries.map(e=>'<div class=abar title="'+esc(e[0])+'：'+e[1]+'"><span class=albl2>'+esc(labelFn?labelFn(e[0]):e[0])+'</span><div class=atrack><i style="width:'+(e[1]/max*100).toFixed(1)+'%;--c:'+(colorFn?colorFn(e[0]):'#2563eb')+'"></i></div><span class=an>'+e[1]+'</span></div>').join('')};
 const stE=this.STATUS_ORDER.filter(k=>s.byStatus[k]).map(k=>[k,s.byStatus[k]]).concat(Object.entries(s.byStatus).filter(e=>this.STATUS_ORDER.indexOf(e[0])<0));
 const tyE=Object.entries(s.byType).sort((a,b)=>b[1]-a[1]);
 const unE=Object.entries(s.byUnit).sort((a,b)=>b[1]-a[1]);
 // 第六波 KVWO-108：主要服务商使用率（在册数前 5，口径：在线/在册（快照），null → 「—」）
 const uTop=(this.units||[]).slice().sort((a,b)=>(b.total||0)-(a.total||0)||String(a.unit).localeCompare(String(b.unit),'zh')).slice(0,5);
 const uHtml=uTop.length?uTop.map(u=>{
  const use=u.usageRate==null?null:Math.round(u.usageRate*1000)/10;
  return '<div class=abar title="'+esc(u.unit)+'：在线 '+(u.online!=null?u.online:'—')+' / 在册 '+(u.total!=null?u.total:'—')+' · 使用率 '+(use==null?'—（不可计算）':use+'%')+'（在线/在册（快照））"><span class=albl2>'+esc(u.unit)+'</span><div class=atrack><i style="width:'+(use==null?0:Math.min(use,100))+'%;--c:#0891b2"></i></div><span class=an>'+(use==null?'—':use+'%')+'</span></div>'
 }).join(''):'<p class=eempty>暂无单位数据</p>';
 el.innerHTML='<div class=adists>'+
  '<div class=apanel><h3>状态分布（在册 '+(s.total!=null?s.total:'—')+' 台）</h3>'+(stE.length?bars(stE,k=>this.ST_COLOR[k]||'#64748b'):'<p class=eempty>暂无数据</p>')+'</div>'+
  '<div class=apanel><h3>设备类型分布</h3>'+(tyE.length?bars(tyE,k=>this.TYPE_COLOR[k]||'#2563eb',k=>this.shortType(k)):'<p class=eempty>暂无数据</p>')+'</div>'+
  '<div class=apanel><h3>所属单位 TOP 分布</h3>'+(unE.length?bars(unE,null):'<p class=eempty>暂无数据</p>')+'</div>'+
  '<div class=apanel><h3>主要服务商使用率（在册数前 5）</h3><p class=uhint style="margin:0 0 10px">口径：在线/在册（快照） · 统计时间：'+esc(this.fmtTs(s.lastSync)||'—')+'</p>'+uHtml+'</div>'+
 '</div>';
},
/* ---------- 模块 2：硬件台账（筛选/自然排序/360°） ---------- */
renderFilters(){
 const s=this.stats;if(!s)return;
 const fill=(id,allLbl,map,ordered)=>{const el=document.getElementById(id);const cur=el.value;
  let keys=Object.keys(map);if(ordered)keys=ordered.filter(k=>map[k]!=null).concat(keys.filter(k=>ordered.indexOf(k)<0));
  el.innerHTML='<option value="">'+allLbl+'</option>'+keys.map(k=>'<option value="'+esc(k)+'">'+esc(id==='asType'?this.shortType(k):k)+'（'+map[k]+'）</option>').join('');
  el.value=keys.indexOf(cur)>=0?cur:''};
 fill('asStatus','全部状态',s.byStatus,this.STATUS_ORDER);fill('asType','全部类型',s.byType);
 const um={};for(const u of this.units||[])um[u.unit]=u.total;fill('asUnit','全部单位',um);
},
sortDevs(list){
 const hw=this.hw;
 return list.slice().sort((x,y)=>{
  if(hw.sortKey==='code')return this.natCmp(x.code,y.code)*hw.sortDir;
  return (this.tsOf(x.updatedAt)-this.tsOf(y.updatedAt))*hw.sortDir||this.natCmp(x.code,y.code)
 })
},
ledgerRows(){
 const hw=this.hw,q=hw.q;
 const match=d=>{
  if(hw.status&&d.status!==hw.status)return false;
  if(hw.type&&d.type!==hw.type)return false;
  if(hw.unit&&d.unit!==hw.unit)return false;
  if(q){const hay=(d.code+' '+(d.remark||'')+' '+(d.unit||'')).toLowerCase();if(hay.indexOf(q)<0)return false}
  return true};
 let list=this.sortDevs(this.devices.filter(match));
 if(hw.archived)list=list.concat(this.sortDevs(this.archived.filter(match)));
 return list
},
ledgerHeadHtml(interactive){
 const cols=[['code','设备编号'],['type','类型'],['status','状态'],['unit','所属单位'],['parts','配件情况'],['remark','去向 / 备注'],['ledgerVer','台账版本'],['detVer','探测版本'],['online','在线'],['idle','闲置'],['openRepairs','维修'],['updatedAt','最后更新']];
 const ar=k=>interactive&&this.hw.sortKey===k?(this.hw.sortDir>0?'▲':'▼'):'';
 return '<tr>'+cols.map(c=>'<th'+(interactive?' class=sort data-sk="'+c[0]+'"':'')+'>'+c[1]+(ar(c[0])?' <span class=sarr>'+ar(c[0])+'</span>':'')+'</th>').join('')+'</tr>'
},
ledgerRowHtml(d){
 const c=this.ST_COLOR[d.status]||'#64748b';
 const on=d.online===true?'<span class=aon title=在线>●</span>':d.online===false?'<span class=aoff title=离线>○</span>':'<span class=anull title=未匹配>—</span>';
 const rep=d.openRepairs>0?'<button class=abadge data-gorep="'+esc(d.code)+'" title="点击查看维修看板">'+d.openRepairs+'</button>':'<span class=anull>0</span>';
 const idl=d.idle===true?'<span class=stag style="--c:#7c3aed" title="库存外·超 '+this.idleDays()+' 天未上线">闲置</span>':'<span class=anull title="不在闲置口径或不可判定">—</span>';
 return '<tr data-code="'+esc(d.code)+'"'+(d.archived?' class=aarch':'')+'><td class=mono>'+esc(d.code.toUpperCase())+'</td><td>'+esc(this.shortType(d.type)||'—')+'</td><td><span class=stag style="--c:'+c+'">'+esc(d.status||'—')+'</span></td><td>'+esc(d.unit||'—')+'</td><td class=wrap title="'+esc(d.parts||'')+'">'+esc(d.parts||'—')+'</td><td class=wrap title="'+esc(d.remark||'')+'">'+esc(d.remark||'—')+'</td><td>'+esc(d.ledgerVer||'—')+'</td><td>'+esc(d.detVer||'—')+'</td><td>'+on+'</td><td>'+idl+'</td><td>'+rep+'</td><td class=mut>'+esc(this.fmtTs(d.updatedAt)||'—')+'</td></tr>'
},
renderLedger(){
 document.getElementById('asLedgerHead').innerHTML=this.ledgerHeadHtml(true);
 const rows=this.ledgerRows();
 document.getElementById('asLedgerBody').innerHTML=rows.length?rows.map(d=>this.ledgerRowHtml(d)).join(''):'<tr><td colspan=12 class=eempty>没有匹配的设备（可调整筛选或关键词）</td></tr>';
 document.getElementById('asLedgerHint').textContent='在册 '+this.devices.length+' 台 · 当前显示 '+rows.filter(d=>!d.archived).length+' 台'+(this.hw.archived?' · 归档 '+rows.filter(d=>d.archived).length+' 台（已追加在下方，淡色显示）':' · 归档 '+this.archived.length+' 台默认隐藏（勾选「显示归档设备」展开）');
},
async open360(code){
 const m=document.getElementById('modal');m.classList.add('show');
 const body=document.getElementById('modalBody');body.innerHTML='<p class=eempty>加载中…</p>';
 let d;
 try{d=await this.api('/api/assets/'+encodeURIComponent(code))}
 catch(e){body.innerHTML='<p class=eempty>读取设备详情失败：'+esc(String(e.message||e))+'</p>';return}
 const led=(d&&(d.ledger||d.device))||d||{};
 const dev=this.normDev(led,!!(d&&d.archived));
 const v=x=>(x==null||x==='')?'—':esc(String(x));
 const tbl=(cols,rows,empty)=>'<div class=twrap><table class=astable><thead><tr>'+cols.map(c=>'<th>'+c+'</th>').join('')+'</tr></thead><tbody>'+(rows.length?rows.join(''):'<tr><td colspan='+cols.length+' class=eempty>'+(empty||'暂无数据')+'</td></tr>')+'</tbody></table></div>';
 const repRows=(d&&d.repairs||[]).map(r=>'<tr><td>'+v(r.stage)+'</td><td class=wrap>'+v(r.issue_description||r.issue)+'</td><td>'+v(Array.isArray(r.repair_owners)?r.repair_owners.join('、'):(r.repair_owners||r.owner))+'</td><td>'+v(this.fmtTs(r.last_followed_at))+'</td><td class=wrap>'+v(r.last_follow_note)+'</td></tr>');
 const flowRows=(d&&d.flows||[]).map(r=>'<tr><td>'+v(r.action)+'</td><td>'+v(r.operator)+'</td><td>'+v(this.fmtTs(r.ts||r.time||r.operated_at))+'</td><td class=wrap>'+v(r.note||r.summary)+'</td></tr>');
 const partRows=(d&&d.parts||[]).map(r=>'<tr><td>'+v(r.part||r.accessory)+'</td><td>'+v(r.arrival||r.arrival_status)+'</td><td>'+v(r.item||r.check_item)+'</td><td class=wrap>'+v(r.note||r.status_note)+'</td><td>'+v(this.fmtTs(r.checked_at||r.time))+'</td></tr>');
 const trRows=(d&&d.transfers||[]).map(r=>'<tr><td>'+v(this.fmtTs(r.ts||r.time))+'</td><td>'+v(r.unit)+'</td><td>'+v(r.item)+'</td><td>'+v(r.qty!=null?r.qty:null)+'</td><td class=wrap>'+v(r.note)+'</td></tr>');
 const stc=this.ST_COLOR[dev.status]||'#64748b';
 body.innerHTML=
  '<header><div><span class=name>'+esc(dev.code.toUpperCase())+'</span> <span class=stag style="--c:'+stc+'">'+esc(dev.status||'—')+'</span>'+(dev.archived?' <span class=stag style="--c:#64748b">归档</span>':'')+'</div><button class=x id=mClose>✕</button></header>'+
  '<h3>台账信息</h3><div class=mgrid>'+
  '<span>设备编号</span><b>'+esc(dev.code.toUpperCase())+'</b>'+
  '<span>设备类型</span><b>'+v(dev.type)+'</b>'+
  '<span>所属单位</span><b>'+v(dev.unit)+'</b>'+
  '<span>配件情况</span><b>'+v(dev.parts)+'</b>'+
  '<span>去向 / 备注</span><b>'+v(dev.remark)+'</b>'+
  '<span>台账软件版本</span><b>'+v(dev.ledgerVer)+'</b>'+
  '<span>探测版本</span><b>'+v(dev.detVer)+'</b>'+
  '<span>在线状态</span><b>'+(dev.online===true?'<span style="color:#16a34a">● 在线</span>':dev.online===false?'<span style="color:#dc2626">○ 离线</span>':'—（未匹配）')+'</b>'+
  '<span>最后在线</span><b>'+(dev.lastSeen?esc(this.fmtTs(dev.lastSeen)):'—（从未上线或 fleet 未启用）')+'</b>'+
  '<span>闲置</span><b>'+(dev.idle===true?'<span style="color:#7c3aed">是（超 '+this.idleDays()+' 天未上线）</span>':dev.idle===false?'否':'—（不可判定）')+'</b>'+
  '<span>未完结维修</span><b>'+(dev.openRepairs||0)+' 条</b>'+
  '<span>最后更新</span><b>'+v(this.fmtTs(dev.updatedAt))+'</b>'+
  '<span>修改人</span><b>'+v(dev.modifier)+'</b>'+
  '</div>'+
  '<h3>维修记录（异常设备表）</h3>'+tbl(['阶段','问题描述','负责人','最后跟进','跟进说明'],repRows)+
  '<h3>处理流水（异常处理流转）</h3>'+tbl(['动作','操作人','时间','说明'],flowRows)+
  '<h3>配件核对</h3>'+tbl(['配件','到货状态','核对项','状态说明','核对时间'],partRows)+
  (trRows.length?'<h3>流转记录</h3>'+tbl(['时间','所属单位','物品','数量','说明'],trRows):'');
 document.getElementById('mClose').onclick=()=>m.classList.remove('show');
},
/* ---------- 模块 3：项目（单位）视图 ---------- */
stagN(l,n){const c=this.ST_COLOR[l]||'#64748b';return '<span class=stag style="--c:'+c+'">'+l+' '+(n==null?'—':n)+'</span>'},
shanghaiInv(){
 if(!this.stats)return null;
 const sh=this.devices.filter(d=>d.unit==='上海办公室');
 const unused=this.stats.invB!=null?this.stats.invB:sh.filter(d=>String(d.remark||'').indexOf('未使用')>=0).length;
 return{unused:unused,newBuild:sh.filter(d=>String(d.remark||'').indexOf('新组装')>=0).length}
},
renderUnits(){
 if(this.drillUnit){this.drill(this.drillUnit);return}
 const el=document.getElementById('asUnitBody'),us=this.units;
 if(!us||!us.length){el.innerHTML='<p class=eempty>暂无单位数据</p>';return}
 const inv=this.shanghaiInv();
 el.innerHTML='<div class=aunitgrid>'+us.map(u=>{
  const rate=u.normalRate==null?null:Math.round(u.normalRate*1000)/10;
  const use=u.usageRate==null?null:Math.round(u.usageRate*1000)/10;
  const st=this.stats||{},isInv=((st.invUnits)||[]).indexOf(u.unit)>=0,icnt=((st.invByUnit)||{})[u.unit];
  return '<div class=aunit data-unit="'+esc(u.unit)+'" title="点击下钻该单位设备列表">'+
   '<div class=auhead><b>'+esc(u.unit)+'</b><span>'+(u.total!=null?u.total:'—')+' 台</span></div>'+
   '<div class=abarate><div class=atrack><i style="width:'+(rate==null?0:Math.min(rate,100))+'%;--c:#16a34a"></i></div><span>'+(rate==null?'—':rate+'%')+' 正常</span></div>'+
   '<div class="abarate aurate" title="使用率 = frps 在线快照 / 在册（在线 '+(u.online!=null?u.online:'—')+' / '+(u.total!=null?u.total:'—')+'，快照口径；fleet 未启用或无法计算时为 —）"><div class=atrack><i style="width:'+(use==null?0:Math.min(use,100))+'%;--c:#0891b2"></i></div><span>'+(use==null?'—':use+'%')+' 使用率</span></div>'+
   '<div class=aurow>'+this.stagN('维修中',u.repairing)+this.stagN('异常',u.abnormal)+'</div>'+
   (isInv?'<div class=auinv>公司库存单位'+(icnt!=null?' '+icnt+' 台':'')+'（不计一线部署）'+(u.unit==='上海办公室'&&inv?' · 备注细分：未使用 '+inv.unused+' · 新组装 '+inv.newBuild:'')+'</div>':'')+
   '<div class=aumut>最近更新：'+esc(this.fmtTs(u.lastUpdated)||'—')+'</div></div>'
 }).join('')+'</div>';
 el.querySelectorAll('.aunit').forEach(c=>c.onclick=()=>this.drill(c.dataset.unit));
},
drill(unit){
 this.drillUnit=unit;this.setTab('units');
 document.getElementById('asUnitBody').hidden=true;
 const det=document.getElementById('asUnitDetail');det.hidden=false;
 const devs=this.sortDevs(this.devices.filter(d=>d.unit===unit));
 const rep=this.devices.filter(d=>d.unit===unit&&(d.status==='维修中'||d.status==='待维修'));
 det.innerHTML='<div class=apanel>'+
  '<div class=auhead2><button class=listbtn id=asUnitBack>← 返回单位列表</button><b>'+esc(unit)+'</b><span>'+devs.length+' 台在册'+(rep.length?' · 维修中/待维修 '+rep.length+' 台':'')+'</span></div>'+
  (rep.length?'<h3>维修中 / 待维修设备</h3><div class=twrap><table class=astable><thead>'+this.ledgerHeadHtml(false)+'</thead><tbody>'+rep.map(d=>this.ledgerRowHtml(d)).join('')+'</tbody></table></div>':'')+
  '<h3>该单位全部设备（点击行查看 360° 详情）</h3>'+
  '<div class=twrap><table class=astable><thead>'+this.ledgerHeadHtml(false)+'</thead><tbody>'+(devs.length?devs.map(d=>this.ledgerRowHtml(d)).join(''):'<tr><td colspan=12 class=eempty>该单位暂无在册设备</td></tr>')+'</tbody></table></div></div>';
 this.writeHash({atab:'units',unit:unit});
},
/* ---------- 模块 4：版本视图 ---------- */
renderVersions(){
 const el=document.getElementById('asVersionBody'),v=this.versions;
 if(!v){el.innerHTML='<p class=eempty>暂无版本数据</p>';return}
 const det=(v.detected||[]).slice().sort((a,b)=>(b.count||0)-(a.count||0));
 const led=(v.ledger||[]).slice().sort((a,b)=>(b.count||0)-(a.count||0));
 const dmax=Math.max.apply(null,det.map(x=>x.count||0).concat([1]));
 const lmax=Math.max.apply(null,led.map(x=>x.count||0).concat([1]));
 const codeOf=x=>typeof x==='string'?x:String((x&&(x.device_code||x.code||x.hostname))||'');
 const chip=x=>{const s=codeOf(x);return '<span class=chip2 title="'+esc(s)+'">'+esc(s.toUpperCase())+'</span>'};
 const only=v.detectedOnly||[],miss=v.ledgerMissing||[];
 // 第六波 KVWO-108：主要服务商版本占比（by_provider_top5，在册数前 5；未探测归「未获取」桶如实展示）
 const t5=v.byProviderTop5||[];
 const provHtml=t5.length?'<div class=apanel><h3>主要服务商版本占比（在册数前 5）</h3><p class=uhint style="margin:0 0 4px">口径：探测版本（未探测计入「未获取」桶） · 占比 = 台数 / 该单位在册数 · 统计时间：'+esc(this.fmtTs(this.stats&&this.stats.lastSync)||'—')+'</p>'+
  t5.map(p=>{
   const vers=(p.versions||[]),vmax=Math.max.apply(null,vers.map(x=>(x&&x.count)||0).concat([1]));
   return '<h4>'+esc(p.unit)+'（在册 '+(p.total!=null?p.total:'—')+' 台）</h4>'+(vers.length?vers.map(x=>{
    const c=(x&&x.count)||0,share=(x&&x.share!=null)?x.share:(p.total?c/p.total:null),spct=share==null?'—':(Math.round(share*1000)/10)+'%';
    const vn=String((x&&x.version)||'—'),isUn=vn==='未获取';
    return '<div class="abar vbar" title="'+esc(p.unit)+' · '+esc(vn)+'：'+c+' 台（占比 '+spct+'）"><span class="albl2 mono'+(isUn?' mut':'')+'">'+esc(vn)+'</span><div class=atrack><i style="width:'+(c/vmax*100).toFixed(1)+'%;--c:'+(isUn?'#94a3b8':'#2563eb')+'"></i></div><span class=an>'+c+' 台 · '+spct+'</span></div>'
   }).join(''):'<p class=eempty>该单位暂无版本数据</p>')
  }).join('')+'</div>':
  '<div class=apanel><h3>主要服务商版本占比（在册数前 5）</h3><p class=eempty>暂无服务商版本分布数据——版本来自 fleet 在线探测（未探测计入「未获取」桶）；后端 by_provider_top5 就绪后此处自动展示，前端无需改动。</p></div>';
 el.innerHTML='<div class=adists>'+
  '<div class=apanel><h3>探测版本分布（在线设备）</h3>'+(det.length?det.map(x=>'<div class=abar><span class="albl2 mono">'+esc(x.version||'—')+'</span><div class=atrack><i style="width:'+((x.count||0)/dmax*100).toFixed(1)+'%"></i></div><span class=an>'+(x.count||0)+'</span></div>').join(''):'<p class=eempty>暂无探测数据</p>')+'</div>'+
  '<div class=apanel><h3>台账登记版本</h3>'+(v.ledgerEmpty?'<div class=uwarn style="margin:0 0 12px">台账「软件版本」当前全部未登记：<b>'+v.ledgerEmpty+'</b> 台为空（如实展示，待台账补录后此处显示分布）</div>':'')+(led.length?led.map(x=>'<div class=abar><span class="albl2 mono">'+esc(x.version||'—')+'</span><div class=atrack><i style="width:'+((x.count||0)/lmax*100).toFixed(1)+'%;--c:#0f766e"></i></div><span class=an>'+(x.count||0)+'</span></div>').join(''):'<p class=eempty>无已登记版本</p>')+'</div>'+
  '<div class=apanel><h3>台账 × 探测对照</h3><div class=acards3><div><b>'+esc(v.matched!=null?v.matched:'—')+'</b>版本匹配</div><div><b>'+only.length+'</b>在线但台账未登记</div><div><b>'+miss.length+'</b>台账有但探测不到</div></div>'+
  '<h4>detected_only · 在线但台账未登记</h4>'+(only.length?only.map(chip).join(''):'<p class=eempty>无</p>')+
  '<h4>ledger_missing · 台账有但探测不到（离线 / 未部署）</h4>'+(miss.length?'<p class=uhint>共 '+miss.length+' 台</p><details><summary style="cursor:pointer;font-size:12px;color:#2563eb">展开设备列表</summary><div style="margin-top:8px">'+miss.map(chip).join('')+'</div></details>':'<p class=eempty>无</p>')+
  '<p style="margin-top:14px"><button class=listbtn id=asGoUpg>▶ 前往升级视图（版本批量升级入口）</button></p></div></div>'+provHtml;
 const b=document.getElementById('asGoUpg');if(b)b.onclick=()=>{location.hash='view=upgrade'};
},
/* ---------- 维修看板子页签：KVWO-80 Rep.* 看板实装（槽位填充，KVWO-88；全站唯一维修入口） ---------- */
renderRepairs(){
 if(this.tab!=='repairs')return;   // 仅在子页签可见时驱动，避免后台空拉数据
 if(typeof Rep==='undefined')return;
 const p=new URLSearchParams(location.hash.slice(1));
 const q=(p.get('view')==='assets'&&p.get('atab')==='repairs')?p.get('q'):'';
 if(q)Rep.enter(q);else Rep.enter();   // hash 携带 q（台账未完结徽章跳转/深链）则应用筛选，否则保持当前状态
},
/* ---------- 契约 mock（仅 404 时启用；确定性种子，数字对齐 KVWO-97 基线：在册 248/归档 69/库存 149（上海 94+潍坊 55）/部署 99/维修率 5.2%/损坏率 8.5%；闲置为确定性抽样） ---------- */
mockFor(path){
 const M=this.mockData();
 if(path.indexOf('/api/assets/stats')===0)return M.stats;
 if(path.indexOf('/api/assets/units')===0)return{units:M.units};
 if(path.indexOf('/api/assets/versions')===0)return M.versions;
 if(path.indexOf('/api/assets?')===0)return{devices:M.archived,last_sync_at:M.lastSync};
 if(path.indexOf('/api/assets/')===0){
  const code=decodeURIComponent(path.slice('/api/assets/'.length)).toLowerCase();
  const dev=M.devices.concat(M.archived).filter(x=>x.device_code.toLowerCase()===code)[0];
  if(dev)return this.mockDetail(dev);
  throw Error('设备不存在：'+code)
 }
 return{devices:M.devices,last_sync_at:M.lastSync};
},
mockData(){
 if(this._mockCache)return this._mockCache;
 let seed=20261020;
 const rnd=()=>{seed|=0;seed=seed+0x6D2B79F5|0;let t=Math.imul(seed^seed>>>15,1|seed);t=t+Math.imul(t^t>>>7,61|t)^t;return((t^t>>>14)>>>0)/4294967296};
 const pickR=a=>a[Math.floor(rnd()*a.length)];
 const UNITS=[['上海办公室',94],['潍坊办公室',55],['苏州项目组',20],['杭州项目组',18],['南京项目组',15],['成都项目组',12],['武汉项目组',10],['西安项目组',8],['北京项目组',6],['广州项目组',6],['长沙项目组',4]];
 const pool=(pairs)=>{const a=[];for(const[s,n]of pairs)for(let i=0;i<n;i++)a.push(s);for(let i=a.length-1;i>0;i--){const j=Math.floor(rnd()*(i+1));const t=a[i];a[i]=a[j];a[j]=t}return a};
 const stPool=pool([['正常',217],['异常',8],['维修中',10],['待维修',3],['待标定',6],['测试中',4]]);
 const tyPool=pool([['EgoPortal-Stereo-V0',160],['EgoPortal-Stereo-V1',55],['EgoPortal-Lite',33]]);
 const PARTS=['头盔','相机','主机','数据线','腰带','充电宝'];
 const base=Date.UTC(2026,9,20,0,0,0);
 const devs=[];let k=0;
 for(const[unit,count]of UNITS)for(let i=0;i<count;i++){
  const status=stPool[k],type=tyPool[k];k++;
  let code='EP-'+String(k).padStart(6,'0');if(k===31)code='ep-000031';   // 大小写混杂样例（匹配须不区分大小写）
  const isInv=unit==='上海办公室'||unit==='潍坊办公室',r=rnd();
  const remark=isInv?(r<0.38?'库存·未使用':r<0.5?'新组装备用':r<0.6?'新组装·待发放':(unit==='上海办公室'?'上海备机':'潍坊备机')):(pickR(['一线部署','现场备用','项目交付','演示用机','']));
  const online=isInv?(r<0.1?null:false):(rnd()<0.72?true:false);
  const detVer=online?pickR(['v0.4.6','v0.4.6','v0.5.0','v0.4.5']):null;
  const openRep=status==='维修中'?(rnd()<0.3?2:1):status==='待维修'?1:(status==='异常'&&rnd()<0.4?1:0);
  devs.push({device_code:code,device_type:type,status:status,unit:unit,accessories:pickR(PARTS)+(rnd()<0.5?'、'+pickR(PARTS):''),remark:remark,ledger_version:null,detected_version:detVer,device_online:online,open_repairs:openRep,updated_at:new Date(base-k*37*60000).toISOString(),modifier:pickR(['张伟','李娜','王强','刘敏','陈晨'])})
 }
 const arch=[];
 for(let i=0;i<69;i++)arch.push({device_code:i===49?'ep-000298':'EP-'+String(249+i).padStart(6,'0'),device_type:pickR(['EgoPortal-Stereo-V0','EgoPortal-Stereo-V1','EgoPortal-Lite']),status:'已拆解',unit:pickR(UNITS.map(u=>u[0])),accessories:'',remark:'已拆解归档',ledger_version:null,detected_version:null,device_online:false,open_repairs:0,updated_at:new Date(base-400*3600000-i*3600000).toISOString(),modifier:'李娜'});
 const byStatus={},byType={},byUnit={};
 for(const s of this.STATUS_ORDER)byStatus[s]=devs.filter(d=>d.status===s).length;
 for(const d of devs){byType[d.device_type]=(byType[d.device_type]||0)+1;byUnit[d.unit]=(byUnit[d.unit]||0)+1}
 // 闲置口径（KVWO-97 需求 4）：库存外·从未上线/超阈值未上线（默认 15 天，KVWO-107）；mock 确定性抽样
 const idleByUnit={};let idleTotal=0;
 for(const d of devs){
  const isInv=d.unit==='上海办公室'||d.unit==='潍坊办公室';
  const idle=!isInv&&d.device_online===false&&rnd()<0.35;
  d.idle=idle;
  d.last_seen=d.device_online===true?String(d.updated_at).slice(0,16).replace('T',' '):(idle?null:new Date(Date.parse(d.updated_at)-5*86400000).toISOString().slice(0,16).replace('T',' '));
  if(idle){idleTotal++;idleByUnit[d.unit]=(idleByUnit[d.unit]||0)+1}
 }
 const invA=(byUnit['上海办公室']||0)+(byUnit['潍坊办公室']||0);
 const invB=devs.filter(d=>(d.unit==='上海办公室'||d.unit==='潍坊办公室')&&String(d.remark||'').indexOf('未使用')>=0).length;
 const det={};for(const d of devs)if(d.detected_version)det[d.detected_version]=(det[d.detected_version]||0)+1;
 // 第六波 KVWO-107 契约：units 增 online/usage_rate（在线/在册）；versions 增 by_provider_top5（在册数前 5 家，未探测归「未获取」桶，share=count/在册保留两位）
 const units=UNITS.map(u=>{const us=devs.filter(d=>d.unit===u[0]);const mx=us.reduce((m,d)=>Math.max(m,Date.parse(d.updated_at)||0),0);const onc=us.filter(d=>d.device_online===true).length;return{unit:u[0],total:us.length,online:onc,usage_rate:us.length?onc/us.length:null,normal_rate:us.length?us.filter(d=>d.status==='正常').length/us.length:null,repairing:us.filter(d=>d.status==='维修中').length,abnormal:us.filter(d=>d.status==='异常').length,last_updated:mx?new Date(mx).toISOString():null}});
 const provTop5=UNITS.slice().sort((a,b)=>b[1]-a[1]||String(a[0]).localeCompare(String(b[0]),'zh')).slice(0,5).map(u=>{
  const us=devs.filter(d=>d.unit===u[0]),cnt={};
  for(const d of us){const k=d.detected_version||'未获取';cnt[k]=(cnt[k]||0)+1}
  return{unit:u[0],total:us.length,versions:Object.entries(cnt).sort((x,y)=>y[1]-x[1]).map(e=>({version:e[0],count:e[1],share:Math.round(e[1]/us.length*100)/100}))}});
 const lastSync=new Date(base+5*60000).toISOString();
 this._mockCache={devices:devs,archived:arch,lastSync:lastSync,
  stats:{registered_total:devs.length,archived_total:arch.length,inventory:{total:invA,by_unit:{'上海办公室':byUnit['上海办公室']||0,'潍坊办公室':byUnit['潍坊办公室']||0},units:['上海办公室','潍坊办公室'],unused_note:invB},deployed:devs.length-invA,idle:{total:idleTotal,threshold_days:15,by_unit:idleByUnit},by_status:byStatus,by_type:byType,by_unit:byUnit,repair_rate:(byStatus['维修中']+byStatus['待维修'])/devs.length,damage_rate:(byStatus['异常']+byStatus['维修中']+byStatus['待维修'])/devs.length,last_sync_at:lastSync},
  units:units,
  versions:{detected:Object.entries(det).map(e=>({version:e[0],count:e[1]})),ledger:[],ledger_empty:devs.length,matched:devs.filter(d=>d.detected_version).length,detected_only:[{device_code:'ep-900201',version:'v0.5.0'},{device_code:'EP-900202',version:'v0.4.6'},{device_code:'EP-900203',version:'v0.5.0'}],ledger_missing:devs.filter(d=>!d.detected_version).map(d=>d.device_code),by_provider_top5:provTop5}};
 return this._mockCache;
},
mockDetail(dev){
 let s=String(dev.device_code).length*7919+42;
 const rnd=()=>{s|=0;s=s+0x6D2B79F5|0;let t=Math.imul(s^s>>>15,1|s);t=t+Math.imul(t^t>>>7,61|t)^t;return((t^t>>>14)>>>0)/4294967296};
 const needRep=dev.status==='维修中'||dev.status==='待维修'||dev.status==='异常'||dev.open_repairs>0;
 const base=(Date.parse(dev.updated_at)||Date.now())-86400000;
 const repairs=needRep?[{stage:dev.status==='待维修'?'待接收':(dev.status==='异常'?'损坏原因确认中':'维修中'),issue_description:dev.device_type+' 相机模块无画面，待进一步检测',repair_owners:['刘希源'],last_followed_at:new Date(base-86400000).toISOString(),last_follow_note:'已联系现场确认寄回地址'}]:[];
 const flows=[{action:'设备登记',operator:'李娜',ts:new Date(base-19*86400000).toISOString(),note:'台账初始登记'},{action:'接收确认',operator:'刘希源',ts:new Date(base-5*86400000).toISOString(),note:'收到设备，外观完好'},{action:'维修跟进',operator:'刘希源',ts:new Date(base-86400000).toISOString(),note:'更换相机模组后复测'}];
 const parts=needRep?[{part:'相机',arrival:rnd()<0.7?'完好':'缺失',item:'相机模组',note:'到货待核对',checked_at:new Date(base-2*86400000).toISOString()}]:[];
 return{ledger:dev,repairs:repairs,flows:flows,parts:parts,transfers:[{ts:new Date(base-14*86400000).toISOString(),unit:dev.unit,item:'整机',qty:1,note:'项目发放'}]};
}
};
/* ================= 维修流转视图（Rep.* 独立命名空间，KVWO-80；并入资产区「维修看板」子页签，KVWO-88） =================
 数据契约（后端 KVWO-79 已合入 data）：
 GET /api/repairs?stage=&type=&unit=&owner=&q= → {repairs:[…17 字段 snake_case +
 record_id/feishu_url/device_online(true|false|null)]}（或直接返回数组）；
 GET /api/repairs/stats → {total,by_stage:{7 阶段:计数},active_count,last_sync_at[,sync_error]}；
 GET /api/repairs/{record_id} → 单条详情（附件含可直接访问的 URL）。
 接口 404 /拉取失败/同步失败：明确提示 + 最后成功同步时间，
 不渲染陈旧数据冒充实时；空态有引导文案；字段缺失一律显示「—」。 */
const Rep={
STAGES:['待接收','损坏原因确认中','维修中','待发出确认','待发出','已发出','已完结'],
SC:{'待接收':'#dc2626','损坏原因确认中':'#d97706','维修中':'#ea580c','待发出确认':'#7c3aed','待发出':'#4f46e5','已发出':'#0891b2','已完结':'#64748b'},
KEYS:{
 device_id:['device_id','device_no','device_code'],
 device_type:['device_type','type'],
 accessories:['accessories','parts'],
 unit:['unit','unit_name','org'],
 stage:['stage','phase'],
 problem:['problem','description','issue'],
 name:['name','contact_name'],
 contact:['contact','phone','mobile','contact_info','contact_phone'],
 receive_address:['receive_address','address','recv_address'],
 return_address:['return_address','return_info','expected_return'],
 tracking_no:['tracking_no','express_no','tracking_number','tracking_no_in'],
 outbound_tracking_no:['outbound_tracking_no','send_tracking_no','outbound_express_no','tracking_no_out'],
 owner:['owner','repair_owner','maintainer','assignee','owner_name'],
 last_followup_at:['last_followup_at','last_follow_time','followup_at','last_followup'],
 last_followup_note:['last_followup_note','last_follow_desc','followup_note','last_note'],
 attachments:['attachments','files'],
 created_at:['created_at','create_time','created'],
 record_id:['record_id','id'],
 feishu_url:['feishu_url','feishu_link','record_url'],
 device_online:['device_online','online']
},
view:'kanban',loading:false,rows:[],all:[],stats:null,statsErr:'',listErr:'',
q:'',stageF:'',typeF:'',unitF:'',ownerF:'',doneOpen:false,qT:null,
gid(i){return document.getElementById(i)},
pick(r,ks){for(const k of ks){const v=r?r[k]:undefined;if(v!==undefined&&v!==null&&v!=='')return v}return null},
parseTs(v){if(v==null)return null;if(typeof v==='number')return v<1e12?v*1000:v;const t=Date.parse(String(v).replace(' ','T'));return isNaN(t)?null:t},
fmtClock(ms){const d=new Date(ms);return isNaN(d.getTime())?'—':d.toLocaleString('zh-CN',{hour12:false})},
fmtRel(ms){if(ms==null)return '—';let s=Math.max(0,(Date.now()-ms)/1000);if(s<60)return Math.floor(s)+' 秒前';const m=s/60;if(m<60)return Math.floor(m)+' 分钟前';const h=m/60;if(h<24)return Math.floor(h)+' 小时前';const d=h/24;if(d<30)return Math.floor(d)+' 天前';if(d<365)return Math.floor(d/30)+' 个月前';return Math.floor(d/365)+' 年前'},
f(v){return v==null||v===''?'—':v},
apiErr(r,d){if(r&&r.status===404)return '接口不可用（404）——请确认服务为含 /api/repairs（KVWO-79）的版本';return (d&&d.error)?d.error:('HTTP '+(r?r.status:'?'))},
norm(r){
 const o={stage:''};
 for(const k in this.KEYS)o[k]=this.pick(r,this.KEYS[k]);
 o.stage=o.stage==null?'':String(o.stage);
 o.stageIdx=this.STAGES.indexOf(o.stage);
 o.done=o.stage==='已完结';
 o.accessories=Array.isArray(o.accessories)?o.accessories.map(String):(o.accessories?String(o.accessories).split(/[,、]/).map(s=>s.trim()).filter(Boolean):[]);
 const atts=Array.isArray(o.attachments)?o.attachments.slice():(o.attachments?[o.attachments]:[]);
 o.attachments=atts.map(a=>{
  if(a==null)return null;
  if(typeof a==='string')return{name:'',url:a};
  const u=String((a.url||a.link||a.src||a.path)||'');
  const nm=String((a.name||a.filename||a.title)||(u?u.split('?')[0].split('/').pop():'')||'附件');
  return{name:nm,url:u};
 }).filter(a=>a&&a.url);
 const dv=o.device_online;
 o.online=(dv===true||dv===1||dv==='true'||dv==='True')?true:((dv===false||dv===0||dv==='false'||dv==='False')?false:null);
 o.followMs=this.parseTs(o.last_followup_at);
 o.createdMs=this.parseTs(o.created_at);
 return o;
},
normList(d){const l=Array.isArray(d)?d:(d&&(d.repairs||d.items||d.records||d.list));return Array.isArray(l)?l:[]},
hasFilter(){return !!(this.q||this.stageF||this.typeF||this.unitF||this.ownerF)},
params(){
 const p=new URLSearchParams();
 if(this.view==='list'&&this.stageF)p.set('stage',this.stageF);
 if(this.typeF)p.set('type',this.typeF);
 if(this.unitF)p.set('unit',this.unitF);
 if(this.ownerF)p.set('owner',this.ownerF);
 if(this.q)p.set('q',this.q);
 const s=p.toString();return s?'?'+s:'';
},
async loadStats(){
 try{
  const r=await fetch('/api/repairs/stats',{cache:'no-store'});
  const d=await r.json().catch(()=>({}));
  if(!r.ok)throw Error(this.apiErr(r,d));
  this.stats={total:d.total,active_count:d.active_count,byStage:d.by_stage||d.byStage||{},syncMs:this.parseTs(d.last_sync_at||d.lastSyncAt),syncError:d.sync_error||d.last_error||d.last_sync_error||''};
  this.statsErr='';
 }catch(e){
  this.statsErr='维修统计不可用：'+((e&&e.message)?e.message:String(e))+(this.stats&&this.stats.syncMs?'（最后成功同步 '+this.fmtClock(this.stats.syncMs)+'，以下统计为当时数据）':'');
 }
 this.renderStats();this.renderBanner();
},
async loadList(){
 this.loading=true;this.render();
 try{
  const r=await fetch('/api/repairs'+this.params(),{cache:'no-store'});
  const d=await r.json().catch(()=>({}));
  if(!r.ok)throw Error(this.apiErr(r,d));
  this.rows=this.normList(d).map(x=>this.norm(x));
  if(!this.params()){this.all=this.rows;this.buildOptions()}
  this.listErr='';
 }catch(e){this.listErr='维修数据加载失败：'+((e&&e.message)?e.message:String(e));this.rows=[]}
 this.loading=false;this.render();
},
load(){this.loadStats();this.loadList()},
enter(pend){
 if(pend){this.q=pend;const qi=this.gid('repQ');if(qi)qi.value=pend}   // 台账未完结徽章跳转/深链携带的设备编号筛入搜索
 this.setView(this.view);this.load()
},
render(){this.renderBanner();if(this.view==='kanban')this.renderKanban();else this.renderList()},
renderStats(){
 const s=this.stats,by=s?s.byStage:null;
 const num=v=>(v==null||v===''||isNaN(Number(v)))?null:Number(v);
 const act=s?num(s.active_count):null;
 const tot=s?num(s.total):null;
 const dn=by?num(by['已完结']):null;
 this.gid('repActive').textContent=act!=null?act:'—';
 this.gid('repTotal').textContent=tot!=null?tot:'—';
 this.gid('repDoneN').textContent=dn!=null?dn:'—';
 const se=this.gid('repSync');
 if(s&&s.syncMs){se.textContent=this.fmtRel(s.syncMs);se.title='最后成功同步：'+this.fmtClock(s.syncMs)}
 else if(s){se.textContent='未同步';se.title='后端尚未完成过一次成功同步'}
 else{se.textContent='—';se.title=''}
 let h='';
 for(const st of this.STAGES){const n=by?num(by[st]):null;h+='<span class="chip rchip" data-stage="'+esc(st)+'" style="--c:'+(this.SC[st]||'#64748b')+'">'+esc(st)+' <b>'+(n!=null?n:'—')+'</b></span>'}
 this.gid('repStageStats').innerHTML=h;
 this.gid('repStageStats').querySelectorAll('.rchip').forEach(c=>c.onclick=()=>{
  const st=c.dataset.stage;
  if(this.view==='list'){const sel=this.gid('repStageF');sel.value=(this.stageF===st)?'all':st;this.stageF=sel.value==='all'?'':sel.value;this.loadList()}
  else{let target=null;this.gid('repKanban').querySelectorAll('.rkcol').forEach(x=>{if(x.dataset.col===st)target=x});if(target)target.scrollIntoView({behavior:'smooth',block:'nearest',inline:'start'})}
 });
},
renderBanner(){
 const el=this.gid('repBanner');
 const errs=[];
 if(this.statsErr)errs.push(this.statsErr);
 if(this.listErr)errs.push(this.listErr);
 if(!errs.length&&this.stats&&this.stats.syncError)errs.push('后端最近一次飞书同步失败：'+this.stats.syncError+(this.stats.syncMs?'（最后成功同步 '+this.fmtClock(this.stats.syncMs)+'）':'（尚无成功同步）'));
 if(!errs.length){el.hidden=true;el.innerHTML='';return}
 el.hidden=false;
 el.innerHTML=errs.map(x=>'<div>'+esc(x)+'</div>').join('')+'<div>当前未展示实时数据；后端同步与接口恢复后，点「重新加载」获取最新。</div>';
},
renderKanban(){
 const el=this.gid('repKanban');
 if(this.listErr){el.innerHTML='<p class="uempty">维修数据不可用，详见上方提示</p>';return}
 const cols=this.STAGES.slice();
 const seen=new Set(this.STAGES);
 for(const r of this.rows){const st=r.stage||'未填写阶段';if(!seen.has(st)){seen.add(st);cols.push(st)}}
 const groups=new Map();for(const st of cols)groups.set(st,[]);
 for(const r of this.rows)groups.get(r.stage||'未填写阶段').push(r);
 let h='';
 if(this.loading&&!this.rows.length)h+='<p class="uempty rkanhint">加载中…</p>';
 else if(!this.rows.length&&this.hasFilter())h+='<p class="uempty rkanhint">没有匹配的维修记录——调整筛选或搜索词，或点「重新加载」</p>';
 else if(!this.rows.length)h+='<p class="uempty rkanhint">暂无维修记录——飞书维修表同步到数据后此处展示</p>';
 for(const st of cols){
  const list=groups.get(st).slice().sort((a,b)=>(b.followMs||0)-(a.followMs||0));
  const c=this.SC[st]||'#64748b';
  h+='<div class="rkcol" data-col="'+esc(st)+'"><h3><span class="rkt" style="--c:'+c+'">'+esc(st)+'</span><span class="rkn">'+list.length+'</span></h3>'+(list.length?list.map(r=>this.cardHtml(r)).join(''):'<div class="rkempty">暂无</div>')+'</div>';
 }
 el.innerHTML=h;
},
cardHtml(r){
 const cls=r.device_type==='双目'?' rt2':(r.device_type==='单目'?' rt1':'');
 const typ=r.device_type?'<span class="rtyp'+cls+'">'+esc(r.device_type)+'</span>':'';
 return '<div class="rcard" data-rid="'+esc(r.record_id)+'">'
  +'<div class="rd">'+esc(r.device_id||'（无编号）')+typ+'</div>'
  +(r.problem?'<div class="rprob" title="'+esc(r.problem)+'">'+esc(r.problem)+'</div>':'')
  +'<div class="rmeta"><span>'+esc(r.owner||'负责人 —')+'</span><span'+(r.followMs?' title="'+esc(this.fmtClock(r.followMs))+'"':'')+'>'+(r.followMs?esc(this.fmtRel(r.followMs)):'跟进 —')+'</span></div>'
  +'<div class="ronrow">'+this.onlineBadge(r)+'</div></div>';
},
onlineBadge(r){
 const did=r.device_id||'';
 if(r.online===true)return '<span class="ron on link" data-jump="'+esc(did)+'" title="设备台账当前在线，点击跳到设备总览定位">台账在线</span>';
 if(r.online===false)return '<span class="ron off link" data-jump="'+esc(did)+'" title="设备台账当前离线，点击跳到设备总览定位">台账离线</span>';
 return '<span class="ron na" title="设备编号未匹配到设备台账">台账 —</span>';
},
renderList(){
 const act=this.rows.filter(r=>!r.done),dn=this.rows.filter(r=>r.done);
 act.sort((a,b)=>(a.stageIdx-b.stageIdx)||((b.followMs||0)-(a.followMs||0)));
 dn.sort((a,b)=>(b.followMs||0)-(a.followMs||0));
 const tb=this.gid('repTbody');
 let msg='';
 if(this.listErr)msg='维修数据不可用，详见上方提示';
 else if(!this.rows.length)msg=this.hasFilter()?'没有匹配的维修记录——调整筛选或搜索词试试':'暂无维修记录——飞书维修表同步到数据后此处展示';
 else if(!act.length)msg='匹配记录均为已完结，见下方折叠区';
 tb.innerHTML=msg?'<tr><td colspan="8" class="rempty">'+esc(msg)+'</td></tr>':act.map(r=>this.rowHtml(r)).join('');
 this.gid('repDoneWrap').hidden=!dn.length;
 this.gid('repDoneTbody').innerHTML=dn.map(r=>this.rowHtml(r)).join('');
 this.gid('repDoneTable').hidden=!this.doneOpen;
 this.gid('repDoneToggle').textContent='已完结记录（'+dn.length+'）'+(this.doneOpen?' 收起 ▴':' 展开 ▾');
},
rowHtml(r){
 return '<tr data-rid="'+esc(r.record_id)+'"><td class="rdev">'+esc(r.device_id||'—')+'</td><td>'+esc(r.device_type||'—')+'</td><td><span class="stag" style="--c:'+(this.SC[r.stage]||'#64748b')+'">'+esc(r.stage||'—')+'</span></td><td>'+esc(r.unit||'—')+'</td><td class="rprob-cell" title="'+esc(r.problem||'')+'">'+esc(r.problem||'—')+'</td><td>'+esc(r.owner||'—')+'</td><td'+(r.followMs?' title="'+esc(this.fmtClock(r.followMs))+'"':'')+'>'+(r.followMs?esc(this.fmtRel(r.followMs)):'—')+'</td><td>'+this.onlineBadge(r)+'</td></tr>';
},
buildOptions(){
 const cnt=key=>{const m=new Map();for(const r of this.all){const v=r[key];if(v)m.set(v,(m.get(v)||0)+1)}return[...m.entries()].sort((a,b)=>a[0].localeCompare(b[0],'zh'))};
 const fill=(id,entries,label,cur)=>{
  const s=this.gid(id);
  s.innerHTML='<option value="all">'+label+'</option>'+entries.map(x=>'<option value="'+esc(x[0])+'">'+esc(x[0])+'（'+x[1]+'）</option>').join('');
  s.value=(cur&&entries.some(x=>x[0]===cur))?cur:'all';
  return s.value==='all'?'':s.value;
 };
 this.unitF=fill('repUnitF',cnt('unit'),'全部单位',this.unitF);
 this.ownerF=fill('repOwnerF',cnt('owner'),'全部负责人',this.ownerF);
 const tmap=new Map([['单目',0],['双目',0],['其他',0]]);
 for(const r of this.all){if(r.device_type)tmap.set(r.device_type,(tmap.get(r.device_type)||0)+1)}
 this.typeF=fill('repTypeF',[...tmap.entries()],'全部类型',this.typeF);
 const sel=this.gid('repStageF');
 sel.innerHTML='<option value="all">全部阶段</option>'+this.STAGES.map(v=>'<option value="'+esc(v)+'">'+esc(v)+'</option>').join('');
 sel.value=(this.stageF&&this.STAGES.includes(this.stageF))?this.stageF:'all';
 this.stageF=sel.value==='all'?'':sel.value;
},
setView(v){
 this.view=v==='list'?'list':'kanban';
 this.gid('repKanban').hidden=this.view!=='kanban';
 this.gid('repListWrap').hidden=this.view!=='list';
 this.gid('repStageF').hidden=this.view!=='list';   // 看板本身按阶段分列，阶段筛选仅列表视图生效
 this.gid('repVKanban').classList.toggle('on',this.view==='kanban');
 this.gid('repVList').classList.toggle('on',this.view==='list');
 this.render();
},
async openDetail(rid){
 rid=rid==null?'':String(rid);
 const cached=this.rows.find(r=>String(r.record_id)===rid)||this.all.find(r=>String(r.record_id)===rid)||null;
 this.gid('repModal').classList.add('show');
 const body=this.gid('repModalBody');
 if(!cached&&!rid){body.innerHTML='<p class="eempty">记录缺少 record_id，无法加载详情</p>';return}
 body.innerHTML='<p class="eempty">加载中…</p>';
 if(!rid){this.renderDetail(cached,true);return}
 try{
  const r=await fetch('/api/repairs/'+encodeURIComponent(rid),{cache:'no-store'});
  const d=await r.json().catch(()=>({}));
  if(!r.ok)throw Error(this.apiErr(r,d));
  let raw=Array.isArray(d)?d[0]:(d&&(d.record||d.repair||d.item)||d);
  this.renderDetail(this.norm((raw&&typeof raw==='object')?raw:{}),false);
 }catch(e){
  if(cached)this.renderDetail(cached,true);
  else{body.innerHTML='<p class="eempty">详情加载失败：'+esc((e&&e.message)?e.message:String(e))+'</p><button type="button" class="x" id="repClose" style="margin-top:10px">关闭</button>';this.bindClose()}
 }
},
bindClose(){const b=this.gid('repClose');if(b)b.onclick=()=>this.closeDetail()},
closeDetail(){this.gid('repModal').classList.remove('show')},
renderDetail(o,degraded){
 const body=this.gid('repModalBody');
 const atts=o.attachments||[];
 const IMG_EXT=['.jpg','.jpeg','.png','.gif','.webp','.bmp'];
 const attHtml=atts.length?'<div class="rattach">'+atts.map(a=>{
  const href=a.url||'';
  const isImg=IMG_EXT.some(e=>href.toLowerCase().split('?')[0].endsWith(e));
  return '<a href="'+esc(href)+'" target="_blank" rel="noopener" title="'+esc(a.name||href)+'">'+(isImg?'<img src="'+esc(href)+'" loading="lazy" alt="'+esc(a.name)+'" onerror="this.style.display=&quot;none&quot;">':'')+'<span class="rfname">'+esc(a.name||'附件')+'</span></a>';
 }).join('')+'</div>':'<p class="eempty">无附件</p>';
 const fu=o.feishu_url?'<a class="rfeishu" href="'+esc(o.feishu_url)+'" target="_blank" rel="noopener">查看飞书原记录 ↗</a>':'<span class="eempty">飞书原记录链接：—</span>';
 const jump=(o.online!==null&&o.device_id)?'<button type="button" id="repJump">在设备总览中定位 '+esc(o.device_id)+'</button>':'';
 body.innerHTML=`
  <header><div><span class="name">${esc(o.device_id||'（无设备编号）')}</span> <span class="stag" style="--c:${this.SC[o.stage]||'#64748b'}">${esc(o.stage||'阶段 —')}</span></div><button class="x" id="repClose">✕</button></header>
  ${degraded?'<p class="uhint warn">详情接口不可用，以下为列表缓存数据（附件可能缺失）</p>':''}
  <div class="mgrid">
   <span>设备编号</span><b>${esc(this.f(o.device_id))}</b>
   <span>设备所属类型</span><b>${esc(this.f(o.device_type))}</b>
   <span>所含配件</span><b>${o.accessories&&o.accessories.length?esc(o.accessories.join('、')):'—'}</b>
   <span>所属单位</span><b>${esc(this.f(o.unit))}</b>
   <span>当前阶段</span><b>${esc(this.f(o.stage))}</b>
   <span>问题描述</span><b style="white-space:pre-wrap">${esc(this.f(o.problem))}</b>
   <span>姓名</span><b>${esc(this.f(o.name))}</b>
   <span>联系方式</span><b>${esc(this.f(o.contact))}</b>
   <span>收件地址</span><b>${esc(this.f(o.receive_address))}</b>
   <span>期望寄回地址和联系方式</span><b>${esc(this.f(o.return_address))}</b>
   <span>快递单号</span><b>${esc(this.f(o.tracking_no))}</b>
   <span>发出快递单号</span><b>${esc(this.f(o.outbound_tracking_no))}</b>
   <span>维修负责人</span><b>${esc(this.f(o.owner))}</b>
   <span>最后跟进时间</span><b>${o.followMs?esc(this.fmtClock(o.followMs)+'（'+this.fmtRel(o.followMs)+'）'):'—'}</b>
   <span>最后跟进说明</span><b style="white-space:pre-wrap">${esc(this.f(o.last_followup_note))}</b>
   <span>创建时间</span><b>${o.createdMs?esc(this.fmtClock(o.createdMs)):'—'}</b>
   <span>台账在线状态</span><b>${this.onlineBadge(o)}</b>
  </div>
  <h3>附件（${atts.length}）</h3>${attHtml}
  <div class="rfootbar">${fu}${jump}</div>`;
 this.bindClose();
 const jb=this.gid('repJump');if(jb)jb.onclick=()=>{this.closeDetail();this.jumpDevice(o.device_id)};
},
jumpDevice(did){
 if(!did)return;
 Upg.setView('overview');
 const qi=document.getElementById('q');
 if(qi)qi.value=did;
 q=String(did).toLowerCase();          // 设备总览全局搜索（matches() 消费）
 siteFilter='';verFilter='';pendingOnly=false;statusFilter='all';
 const sf=document.getElementById('statusFilter');if(sf)sf.value='all';
 renderSites();renderVers();renderGrid();
 if(qi)qi.scrollIntoView({behavior:'smooth',block:'center'});
},
init(){
 this.gid('repKanban').addEventListener('click',e=>{
  const j=e.target.closest('.ron.link');if(j){e.stopPropagation();this.jumpDevice(j.dataset.jump);return}
  const c=e.target.closest('.rcard');if(c)this.openDetail(c.dataset.rid);
 });
 const rowClick=e=>{
  const j=e.target.closest('.ron.link');if(j){e.stopPropagation();this.jumpDevice(j.dataset.jump);return}
  const tr=e.target.closest('tr[data-rid]');if(tr)this.openDetail(tr.dataset.rid);
 };
 this.gid('repTbody').addEventListener('click',rowClick);
 this.gid('repDoneTbody').addEventListener('click',rowClick);
 this.gid('repVKanban').onclick=()=>this.setView('kanban');
 this.gid('repVList').onclick=()=>this.setView('list');
 this.gid('repReload').onclick=()=>this.load();
 this.gid('repDoneToggle').onclick=()=>{this.doneOpen=!this.doneOpen;this.renderList()};
 const qi=this.gid('repQ');
 qi.addEventListener('input',()=>{clearTimeout(this.qT);this.qT=setTimeout(()=>{this.q=qi.value.trim();this.loadList()},350)});
 for(const id of['repStageF','repTypeF','repUnitF','repOwnerF']){
  this.gid(id).addEventListener('change',()=>{
   const g=i=>{const v=this.gid(i).value;return v==='all'?'':v};
   this.stageF=g('repStageF');this.typeF=g('repTypeF');this.unitF=g('repUnitF');this.ownerF=g('repOwnerF');
   this.loadList();
  });
 }
 const m=this.gid('repModal');
 m.addEventListener('click',e=>{if(e.target===m)this.closeDetail()});
 document.addEventListener('keydown',e=>{if(e.key==='Escape')this.closeDetail()});
 setInterval(()=>{if(!this.gid('atabRepairs').hidden)this.load()},60000);   // 资产区维修看板子页签可见时刷新
}
};
Upg.init();      // 路由器先就绪（setView 引用下方 Assets/Rep，声明先于本行求值）
Assets.init();
Rep.init();
/* 会话过期兜底（2026-10-26 登录页）：SPA 任意 fetch 命中 401 → 整页跳登录页 */
(function(){var f=window.fetch;if(!f)return;window.fetch=function(){return f.apply(this,arguments).then(function(r){if(r.status===401&&location.pathname.indexOf('/login')!==0)location.href='/login';return r})}})();
