// One live connection per tab: a server-sent event stream with a status snapshot
// every second and every pipeline/agent event as it happens. Components subscribe
// with useLive(); React Query caches are refreshed when an event touches them.

import { useSyncExternalStore } from "react";
import { QueryClient } from "@tanstack/react-query";
import { api, type Point, type Status, type StoredEvent } from "./api";

type LiveState = {
  connected: boolean;
  status: Status | null;
  series: Point[];
  events: StoredEvent[];   // newest last
};

const SERIES_S = 300;
let state: LiveState = { connected: false, status: null, series: [], events: [] };
const listeners = new Set<() => void>();
let source: EventSource | null = null;
let lastId = 0;

function set(patch: Partial<LiveState>) {
  state = { ...state, ...patch };
  listeners.forEach((l) => l());
}

function addPoint(p: Point) {
  const series = state.series.filter((x) => x.t !== p.t && x.t > p.t - SERIES_S);
  series.push(p);
  series.sort((a, b) => a.t - b.t);
  return series;
}

export function startLive(queryClient: QueryClient) {
  if (source) return;
  Promise.all([api.metrics(SERIES_S), api.status(), api.events(0)])
    .then(([series, status, events]) => {
      lastId = Math.max(lastId, ...events.map((e) => e.id), 0);
      set({ series, status, events: events.slice(-300) });
    })
    .catch(() => { /* the stream below retries on its own */ });

  const connect = () => {
    source = new EventSource(`/api/stream${lastId ? `?after=${lastId}` : ""}`);
    source.onopen = () => set({ connected: true });
    source.onerror = () => set({ connected: false });
    source.addEventListener("metrics", (msg) => {
      const d = JSON.parse((msg as MessageEvent).data);
      set({ status: d.status, series: addPoint(d.point) });
    });
    source.addEventListener("event", (msg) => {
      const ev: StoredEvent = JSON.parse((msg as MessageEvent).data);
      if (ev.id <= lastId && state.events.some((e) => e.id === ev.id)) return;
      lastId = Math.max(lastId, ev.id);
      set({ events: [...state.events, ev].slice(-300) });
      queryClient.invalidateQueries({ queryKey: ["incidents"] });
      if (ev.incident_id) queryClient.invalidateQueries({ queryKey: ["incident", ev.incident_id] });
      if (ev.kind === "drift_changed") queryClient.invalidateQueries({ queryKey: ["chaos"] });
    });
  };
  connect();
}

export function useLive(): LiveState {
  return useSyncExternalStore(
    (cb) => { listeners.add(cb); return () => listeners.delete(cb); },
    () => state,
  );
}
