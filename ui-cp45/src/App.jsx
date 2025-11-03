import { useEffect, useMemo, useState } from "react";
import { LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, Treemap, Cell, AreaChart, Area } from "recharts";
import { Star, Settings } from "lucide-react";

const Colors = { text:"#D7DBE2", textDim:"#9AA3B2", panel:"#141522", grid:"#2A2C3D" };
const SERIES_COLORS = ["#58A6FF","#A78BFA","#22C55E","#EF4444","#10B981","#F97316","#EAB308","#06B6D4","#9B59B6"];

function Panel({title,right,children}) {
  return (
    <div className="rounded-2xl shadow-lg p-3 md:p-4" style={{background:Colors.panel,color:Colors.text}}>
      <div className="flex items-center justify-between pb-3">
        <h3 className="text-sm md:text-base font-semibold opacity-90">{title}</h3>
        <div className="flex items-center gap-2">{right}</div>
      </div>
      {children}
    </div>
  );
}

function TimeframeToggle({value,onChange}) {
  const opts = ["1D","1W","1M","3M","1Y"];
  return (
    <div className="flex gap-1">
      {opts.map(o=>(
        <button key={o} onClick={()=>onChange(o)}
          className={`px-2 py-1 rounded-md text-xs border ${value===o?'bg-white/10 border-white/20 text-white':'bg-transparent border-white/10 text-[#9AA3B2] hover:text-white'}`}>
          {o}
        </button>
      ))}
    </div>
  );
}

function Sparkline({ data }) {
  const points = data.map((y,i)=>({i,y}));
  return (
    <div className="h-10 w-40">
      <ResponsiveContainer width="100%" height="100%">
        <AreaChart data={points} margin={{top:4,right:0,left:0,bottom:0}}>
          <Area type="monotone" dataKey="y" stroke="#7DD3FC" fill="#7DD3FC22" strokeWidth={2}/>
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
}

export default function App() {
  const [tf,setTf] = useState("1D");
  const [series,setSeries] = useState([]);
  const [tree,setTree] = useState(null);
  const [rows,setRows] = useState([]);

  const API = (import.meta.env.VITE_API_BASE ?? "");

  useEffect(()=> {
    Promise.all([
      fetch(`${API}/api/categories/indices?tf=${tf}`).then(r=>r.json()),
      fetch(`${API}/api/categories/heatmap?window=24h`).then(r=>r.json()),
      fetch(`${API}/api/markets/top?limit=25`).then(r=>r.json()),
    ]).then(([s,t,r])=>{ setSeries(s); setTree(t); setRows(r); })
     .catch(err => console.error("Fetch error:", err));
  },[tf, API]);

  const chartData = useMemo(()=>{
    const len = series[0]?.points.length || 0;
    return Array.from({length:len},(_,i)=>{
      const o = {i};
      series.forEach(s=> o[s.label] = s.points[i].v);
      return o;
    });
  },[series]);

  const toggleStar = (i) => setRows(prev => prev.map((r,idx)=> idx===i ? {...r, starred: !r.starred} : r));

  return (
    <div className="min-h-screen w-full" style={{background:"#0e0f14"}}>
      <div className="max-w-[1400px] mx-auto p-3 md:p-6">

        {/* TOP ROW */}
        <div className="grid grid-cols-12 gap-4">
          <div className="col-span-12 lg:col-span-8">
            <Panel title="Category indices" right={<TimeframeToggle value={tf} onChange={setTf}/>}>
              <div className="h-[280px] md:h-[320px]">
                <ResponsiveContainer width="100%" height="100%">
                  <LineChart data={chartData} margin={{top:8,right:16,left:0,bottom:0}}>
                    <XAxis dataKey="i" hide />
                    <YAxis tick={{ fill: Colors.textDim, fontSize: 11 }} stroke={Colors.grid}/>
                    <Tooltip contentStyle={{ background: Colors.panel, border: `1px solid ${Colors.grid}`, color: Colors.text }}/>
                    {series.map((s,idx)=>(
                      <Line key={s.label} type="monotone" dataKey={s.label} stroke={SERIES_COLORS[idx % SERIES_COLORS.length]} dot={false} strokeWidth={2}/>
                    ))}
                  </LineChart>
                </ResponsiveContainer>
              </div>
              <div className="mt-3 flex flex-wrap gap-2 text-xs text-[#9AA3B2]">
                {series.map((s,idx)=>(
                  <span key={s.label} className="inline-flex items-center gap-2">
                    <span className="w-3 h-1 rounded-full" style={{background:SERIES_COLORS[idx % SERIES_COLORS.length]}}></span> {s.label}
                  </span>
                ))}
              </div>
            </Panel>
          </div>
          <div className="col-span-12 lg:col-span-4">
            <Panel title="Category indices heatmap" right={<span className="text-xs text-[#9AA3B2]">Up/Down</span>}>
              <div className="h-[320px]">
                {tree && (
                  <ResponsiveContainer width="100%" height="100%">
                    <Treemap data={tree.children} dataKey="size" stroke="#0e0f14">
                      {tree.children.map((entry,i)=>(<Cell key={i} fill={entry.fill} stroke="#0e0f14" />))}
                    </Treemap>
                  </ResponsiveContainer>
                )}
              </div>
            </Panel>
          </div>
        </div>

        {/* TOOLBAR */}
        <div className="mt-4 flex flex-wrap gap-2 text-xs">
          {["Favorites","Hot","New","Margin","Earn","Ecosystems","Quote"].map((label)=>(
            <button key={label} className="px-3 py-1.5 rounded-lg bg-white/5 text-[#9AA3B2] hover:text-white hover:bg-white/10 border border-white/10">{label}</button>
          ))}
          <div className="ml-auto"/>
          <button className="px-3 py-1.5 rounded-lg bg-white/5 text-white border border-white/10 inline-flex items-center gap-2">
            <Settings className="w-4 h-4"/> View settings
          </button>
        </div>

        {/* TABLE */}
        <div className="mt-4 rounded-2xl overflow-hidden border border-white/10" style={{background:"#141522", color:"#D7DBE2"}}>
          <div className="grid grid-cols-12 px-4 py-3 text-[10px] md:text-xs uppercase tracking-wide text-[#9AA3B2]">
            <div className="col-span-3">Market</div>
            <div className="col-span-2">Base name</div>
            <div className="col-span-2">Quote name</div>
            <div className="col-span-2">Category</div>
            <div className="col-span-1 text-right">Price</div>
            <div className="col-span-1 text-right">24h Chg.</div>
            <div className="col-span-1 text-right">24h Vol.</div>
          </div>
          <div className="divide-y divide-white/10">
            {rows.map((r,idx)=>(
              <div key={idx} className="grid grid-cols-12 items-center px-4 py-3 text-sm hover:bg-white/5">
                <div className="col-span-3 flex items-center gap-2">
                  <div className="w-6 h-6 rounded-full bg-white/10" />
                  <div className="flex flex-col">
                    <span className="font-medium">{r.market}</span>
                    <span className="text-[11px] text-[#9AA3B2]">{r.base}</span>
                  </div>
                </div>
                <div className="col-span-2 text-[#9AA3B2]">{r.base}</div>
                <div className="col-span-2 text-[#9AA3B2]">{r.quote}</div>
                <div className="col-span-2 text-[#9AA3B2]">{r.category}</div>
                <div className="col-span-1 text-right tabular-nums">{Intl.NumberFormat().format(r.price)}</div>
                <div className={`col-span-1 text-right tabular-nums ${r.change>=0?'text-emerald-400':'text-rose-400'}`}>{r.change.toFixed(2)}%</div>
                <div className="col-span-1 text-right flex items-center gap-3 justify-end">
                  <Sparkline data={r.spark}/>
                  <div>{r.volume.toFixed(1)}M USD</div>
                  <button onClick={()=>toggleStar(idx)} title="Toggle favorite">
                    <Star className={`w-4 h-4 ${r.starred?'text-amber-400':'text-[#9AA3B2]'}`}/>
                  </button>
                </div>
              </div>
            ))}
          </div>
        </div>

        {/* TICKER */}
        <div className="mt-4 text-[11px] text-[#9AA3B2] flex items-center gap-4 overflow-x-auto">
          {["BTC/USD 0.93%","DOGE/USD 0.37%","PENGU/USD 3.49%","SOL/USD 0.69%","SPX/USD 2.94%","STBL/USD 1.64%","XRP/USD 4.19%","ZEC/USD 5.12%"].map((t,i)=>(
            <span key={i} className="px-2 py-1 rounded bg-white/5 whitespace-nowrap">{t}</span>
          ))}
        </div>
      </div>
    </div>
  );
}
