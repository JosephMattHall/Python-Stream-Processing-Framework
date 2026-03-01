DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>PSPF | Worker Dashboard</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700&family=JetBrains+Mono&display=swap" rel="stylesheet">
    <style>
        body { font-family: 'Inter', sans-serif; background: #0f172a; color: #f8fafc; }
        .glass { background: rgba(30, 41, 59, 0.7); backdrop-filter: blur(12px); border: 1px solid rgba(255, 255, 255, 0.1); }
        .accent-gradient { background: linear-gradient(135deg, #38bdf8 0%, #818cf8 100%); }
        .pulse { animation: pulse 2s cubic-bezier(0.4, 0, 0.6, 1) infinite; }
        @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: .5; } }
        .code { font-family: 'JetBrains Mono', monospace; }
    </style>
</head>
<body class="p-6 md:p-12">
    <div class="max-w-6xl mx-auto space-y-8">
        <!-- Header -->
        <header class="flex flex-col md:flex-row justify-between items-start md:items-center gap-4">
            <div>
                <h1 class="text-4xl font-bold tracking-tight bg-clip-text text-transparent bg-gradient-to-r from-sky-400 to-indigo-400">
                    PSPF Worker
                </h1>
                <p class="text-slate-400 mt-1">Real-time Stream Processor Insight</p>
            </div>
            <div id="status-badge" class="glass px-4 py-2 rounded-full flex items-center gap-3">
                <div class="w-3 h-3 rounded-full bg-slate-500" id="status-indicator"></div>
                <span id="status-text" class="font-semibold text-sm uppercase tracking-wider">Connecting...</span>
            </div>
        </header>

        <!-- Stats Grid -->
        <div class="grid grid-cols-1 md:grid-cols-3 gap-6">
            <div class="glass p-6 rounded-2xl">
                <p class="text-slate-400 text-sm font-medium uppercase tracking-wide">Backend</p>
                <p id="backend-val" class="text-2xl font-bold mt-2 code text-sky-400">---</p>
            </div>
            <div class="glass p-6 rounded-2xl">
                <p class="text-slate-400 text-sm font-medium uppercase tracking-wide">Node ID</p>
                <p id="node-id-val" class="text-2xl font-bold mt-2 code text-indigo-400 truncate">---</p>
            </div>
            <div class="glass p-6 rounded-2xl">
                <p class="text-slate-400 text-sm font-medium uppercase tracking-wide">Uptime</p>
                <p id="uptime-val" class="text-2xl font-bold mt-2">0s</p>
            </div>
        </div>

        <!-- Controls -->
        <div class="glass p-8 rounded-2xl space-y-6">
            <h2 class="text-xl font-bold border-b border-white/10 pb-4">Worker Controls</h2>
            <div class="flex flex-wrap gap-4">
                <button onclick="control('pause')" class="px-6 py-3 rounded-xl bg-orange-500/20 text-orange-400 border border-orange-500/30 hover:bg-orange-500/30 transition-all font-semibold">
                    Pause Consumption
                </button>
                <button onclick="control('resume')" class="px-6 py-3 rounded-xl bg-emerald-500/20 text-emerald-400 border border-emerald-500/30 hover:bg-emerald-500/30 transition-all font-semibold">
                    Resume Consumption
                </button>
            </div>
        </div>

        <!-- Cluster View -->
        <div class="grid grid-cols-1 lg:grid-cols-2 gap-8">
            <div class="glass p-8 rounded-2xl">
                <h2 class="text-xl font-bold mb-6 flex items-center gap-2">
                    <svg class="w-5 h-5 text-sky-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10"/></svg>
                    Cluster Topology
                </h2>
                <div id="cluster-list" class="space-y-4">
                    <p class="text-slate-500 italic">Scanning cluster...</p>
                </div>
            </div>
            <div class="glass p-8 rounded-2xl">
                <h2 class="text-xl font-bold mb-6 flex items-center gap-2">
                    <svg class="w-5 h-5 text-indigo-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m5.618-4.016A11.955 11.955 0 0112 2.944a11.955 11.955 0 01-8.618 3.04A12.02 12.02 0 003 9c0 5.591 3.824 10.29 9 11.622 5.176-1.332 9-6.03 9-11.622 0-1.042-.133-2.052-.382-3.016z"/></svg>
                    Held Partitions
                </h2>
                <div id="partition-grid" class="flex flex-wrap gap-2">
                    <p class="text-slate-500 italic">Detecting assignments...</p>
                </div>
            </div>
        </div>

        <footer class="text-center text-slate-500 text-sm py-8 border-t border-white/5">
            PSPF v1.0 Production Release &bull; Built for Simplicity and Reliability
        </footer>
    </div>

    <script>
        const API_BASE = window.location.origin;
        
        async function refresh() {
            try {
                // Status & Backend
                const statusResp = await fetch(`${API_BASE}/control/status`);
                const status = await statusResp.json();
                
                document.getElementById('backend-val').innerText = status.backend;
                const statusTxt = document.getElementById('status-text');
                const statusInd = document.getElementById('status-indicator');
                
                if (status.running && !status.paused) {
                    statusTxt.innerText = 'Running';
                    statusTxt.className = 'font-semibold text-sm uppercase tracking-wider text-emerald-400';
                    statusInd.className = 'w-3 h-3 rounded-full bg-emerald-500 shadow-[0_0_10px_rgba(16,185,129,0.5)]';
                } else if (status.paused) {
                    statusTxt.innerText = 'Paused';
                    statusTxt.className = 'font-semibold text-sm uppercase tracking-wider text-orange-400';
                    statusInd.className = 'w-3 h-3 rounded-full bg-orange-500';
                } else {
                    statusTxt.innerText = 'Stopped';
                    statusTxt.className = 'font-semibold text-sm uppercase tracking-wider text-red-500';
                    statusInd.className = 'w-3 h-3 rounded-full bg-red-500';
                }

                // Cluster & Partitions
                const clusterResp = await fetch(`${API_BASE}/cluster/status`);
                const cluster = await clusterResp.json();
                
                document.getElementById('node-id-val').innerText = cluster.node_id || 'Standalone';
                
                const clusterList = document.getElementById('cluster-list');
                clusterList.innerHTML = '';
                
                // Self
                const selfDiv = document.createElement('div');
                selfDiv.className = 'flex justify-between items-center p-4 rounded-xl bg-white/5 border border-white/10';
                selfDiv.innerHTML = `<span class="font-semibold text-sky-400">Node ${cluster.node_id?.slice(0,8) || 'LOCAL'} (You)</span><span class="text-xs bg-emerald-500/20 text-emerald-400 px-2 py-1 rounded">LEADER</span>`;
                clusterList.appendChild(selfDiv);

                cluster.nodes?.forEach(node => {
                    const nodeDiv = document.createElement('div');
                    nodeDiv.className = 'flex justify-between items-center p-4 rounded-xl bg-white/5 border border-dashed border-white/10 opacity-70';
                    nodeDiv.innerHTML = `<span>Node ${node.id.slice(0,8)}</span><span class="text-xs text-slate-500">${node.host}:${node.port}</span>`;
                    clusterList.appendChild(nodeDiv);
                });

                const partGrid = document.getElementById('partition-grid');
                partGrid.innerHTML = '';
                if (cluster.held_partitions?.length) {
                    cluster.held_partitions.forEach(p => {
                        const pDiv = document.createElement('div');
                        pDiv.className = 'px-3 py-1 rounded-lg bg-indigo-500/20 text-indigo-300 border border-indigo-500/30 text-xs font-bold';
                        pDiv.innerText = `P ${p}`;
                        partGrid.appendChild(pDiv);
                    });
                } else {
                    partGrid.innerHTML = '<p class="text-slate-500 italic text-sm">No partitions held</p>';
                }

            } catch (err) {
                console.error("Refresh failed", err);
            }
        }

        async function control(action) {
            await fetch(`${API_BASE}/control/${action}`, { method: 'POST' });
            refresh();
        }

        setInterval(refresh, 2000);
        refresh();
    </script>
</body>
</html>
"""
