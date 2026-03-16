import axios from "axios";

const API_BASE = ""; // Use relative path for Vite proxy
const BACKEND_BASE = "http://localhost:8000";

export const uploadZip = (file) => {
  const form = new FormData();
  form.append("file", file);
  return axios.post(`${API_BASE}/upload`, form, {
    headers: { "Content-Type": "multipart/form-data" },
  });
};

export const startProcessing = (data) => {
  // data: { path: "..."} or { session_id: "..." }
  return axios.post(`${API_BASE}/start`, data);
};

export const stopProcessing = () => {
  return axios.post(`${API_BASE}/stop`);
};

export const fetchMetrics = () => {
  return axios.get(`${API_BASE}/metrics`);
};

export const fetchMarkdown = (sessionId) => {
  return axios.get(`${API_BASE}/markdown/${sessionId}`);
};

export const fetchGraphMetrics = () =>
  axios.get(`${BACKEND_BASE}/metadata/graph/metrics`);

export const fetchVisualGraph = () =>
  fetch(`${BACKEND_BASE}/metadata/graph/visual`)
    .then(res => res.json());

export const fetchLineageRoots = () =>
  fetch(`${BACKEND_BASE}/lineage/graph/roots`).then((res) => {
    if (!res.ok) throw new Error(`Lineage roots API error: ${res.status}`);
    return res.json();
  });

export const fetchLineageFullGraph = () =>
  fetch(`${BACKEND_BASE}/lineage/graph/full`).then((res) => {
    if (!res.ok) throw new Error(`Lineage full graph API error: ${res.status}`);
    return res.json();
  });

export const fetchLineageLayerExpand = (layerId, includeNeighbors = true) =>
  fetch(
    `${BACKEND_BASE}/lineage/graph/layer/${encodeURIComponent(layerId)}?include_neighbors=${includeNeighbors}`
  ).then((res) => {
    if (!res.ok) throw new Error(`Lineage layer expand API error: ${res.status}`);
    return res.json();
  });

export const fetchLineageTableExpand = (tableId) =>
  fetch(`${BACKEND_BASE}/lineage/graph/table/${encodeURIComponent(tableId)}`).then((res) => {
    if (!res.ok) throw new Error(`Lineage table expand API error: ${res.status}`);
    return res.json();
  });

export const fetchCanvasLineageGraph = () =>
  fetch(`${BACKEND_BASE}/metadata/graph/canvas`)
    .then((res) => {
      if (!res.ok) throw new Error(`Canvas API error: ${res.status}`);
      return res.json();
    });

export const fetchTableCanvasLineage = () =>
  fetch(`${BACKEND_BASE}/metadata/canvas`)
    .then((res) => {
      if (!res.ok) throw new Error(`Table canvas API error: ${res.status}`);
      return res.json();
    });

export const fetchColumnCanvasLineage = () =>
  fetch(`${BACKEND_BASE}/metadata/columns`)
    .then((res) => {
      if (!res.ok) throw new Error(`Column canvas API error: ${res.status}`);
      return res.json();
    });

export const dropAllMetadataGraph = () =>
  fetch(`${BACKEND_BASE}/metadata/admin/drop-all`, { method: "DELETE" })
    .then((res) => {
      if (!res.ok) throw new Error(`Drop graph API error: ${res.status}`);
      return res.json();
    });
