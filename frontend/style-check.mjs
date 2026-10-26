import { Window } from 'happy-dom'
import fs from 'node:fs'
const win = new Window({ url: 'http://t.test/' })
// 仅做 CSS 解析与选择器命中验证（不跑 bundle）：注入构建 CSS + 最小 DOM 骨架
const css = fs.readFileSync(fs.readdirSync('dist/assets').filter(f=>f.endsWith('.css')).map(f=>'dist/assets/'+f)[0],'utf8')
const style = win.document.createElement('style'); style.textContent = css; win.document.head.appendChild(style)
win.document.body.innerHTML = `<section id=viewAssets>
<div class=amcards><div class=amcard style="--c:#0f766e"><span class=albl>库存</span><b>149</b></div></div>
<div class=rkcol style="--c:#dc2626"><h3><span class=rkt>待接收</span></h3><div class=rcard><div class=rd>EP-1</div></div></div>
<div class=vtabs><span class=asub></span></div></section>`
const cs = sel => win.getComputedStyle(win.document.querySelector(sel))
console.log('amcard 圆角:', cs('.amcard').borderRadius, '| 卡片阴影:', cs('.amcard').boxShadow.slice(0, 40))
console.log('rkcol 背景:', cs('.rkcol').backgroundColor, '最小宽:', cs('.rkcol').minWidth)
console.log('rkt 字色(应带--c):', cs('.rkt').color)
