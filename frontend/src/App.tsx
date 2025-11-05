import React, { useEffect, useMemo, useRef, useState } from 'react'

type Pipeline = { id: string; name: string }
type PipelineRun = { id: string; pipeline_id: string; status: string }
type LogEntry = { id: string; pipeline_run_id: string; block_run_id?: string; level: string; message: string; timestamp: string }

const API_URL = (import.meta.env.VITE_API_URL as string) || 'http://localhost:8000'

export default function App() {
    const [pipelines, setPipelines] = useState<Pipeline[]>([])
    const [runs, setRuns] = useState<Record<string, PipelineRun[]>>({})
    const [logs, setLogs] = useState<LogEntry[]>([])
    const eventSources = useRef<Record<string, EventSource>>({})

    // Fetch pipelines
    useEffect(() => {
        fetch(`${API_URL}/pipelines`).then(r => r.json()).then(setPipelines).catch(() => setPipelines([]))
    }, [])

    // Fetch runs per pipeline and refresh periodically
    useEffect(() => {
        let cancelled = false
        async function refreshRuns() {
        const all: Record<string, PipelineRun[]> = {}
        for (const p of pipelines) {
            try {
            const r = await fetch(`${API_URL}/pipelines/${p.id}/runs`).then(res => res.json())
            all[p.id] = r
            } catch {
            all[p.id] = []
            }
        }
        if (!cancelled) setRuns(all)
        }
        if (pipelines.length) {
        refreshRuns()
        const t = setInterval(refreshRuns, 5000)
        return () => { cancelled = true; clearInterval(t) }
        }
    }, [pipelines])

    // Open SSE for new runs and keep them open
    useEffect(() => {
        const existing = eventSources.current
        const allRunIds = Object.values(runs).flat().map(r => r.id)
        // start new sources
        for (const runId of allRunIds) {
        if (!existing[runId]) {
            const es = new EventSource(`${API_URL}/logs/${runId}`)
            es.onmessage = (ev) => {
            try {
                const entry: LogEntry = JSON.parse(ev.data)
                setLogs(prev => [...prev, entry])
            } catch {}
            }
            es.onerror = () => { /* keep trying, browser will reconnect */ }
            existing[runId] = es
        }
        }
        // clean up sources for runs that no longer exist
        for (const [runId, es] of Object.entries(existing)) {
        if (!allRunIds.includes(runId)) { es.close(); delete existing[runId] }
        }
    }, [runs])

    const sortedLogs = useMemo(() => logs.slice().sort((a, b) => a.timestamp.localeCompare(b.timestamp)), [logs])

    return (
        <div>
        <h1>Charcentric Live Logs</h1>
        <section>
            <h2>Pipelines</h2>
            <ul>
            {pipelines.map(p => (
                <li key={p.id}>{p.name} ({p.id})</li>
            ))}
            </ul>
        </section>
        <section>
            <h2>Runs</h2>
            {pipelines.map(p => (
            <div key={p.id}>
                <h3>{p.name}</h3>
                <ul>
                {(runs[p.id] || []).map(r => (
                    <li key={r.id}>{r.id} — {r.status}</li>
                ))}
                </ul>
            </div>
            ))}
        </section>
        <section>
            <h2>Live Logs</h2>
            <pre style={{ whiteSpace: 'pre-wrap' }}>
    {sortedLogs.map(l => `${l.timestamp} [${l.level}] run=${l.pipeline_run_id}${l.block_run_id ? ` block=${l.block_run_id}` : ''} :: ${l.message}`).join('\n')}
            </pre>
        </section>
        </div>
    )
}


