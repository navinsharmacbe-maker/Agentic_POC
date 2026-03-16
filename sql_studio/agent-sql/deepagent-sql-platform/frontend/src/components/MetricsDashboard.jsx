import React, { useEffect, useState } from "react";
import { fetchGraphMetrics } from "../services/api";

export default function MetricsDashboard() {
  const [metrics, setMetrics] = useState({
    layers: [],
    summary: {
      layers: 0,
      tables: 0,
      versions: 0,
      columns: 0,
    },
  });

  useEffect(() => {
    const loadMetrics = async () => {
      try {
        const res = await fetchGraphMetrics();
        console.log("Graph Metrics API:", res.data);
        setMetrics(res.data);
      } catch (err) {
        console.error("Graph metrics fetch failed", err);
      }
    };

    loadMetrics();
    const id = setInterval(loadMetrics, 5000);
    return () => clearInterval(id);
  }, []);

  return (
    <div>
      <h3>Neo4j Graph Metrics</h3>

      <h4>Summary</h4>
      <p>Total Layers: {metrics.summary.layers}</p>
      <p>Total Tables: {metrics.summary.tables}</p>
      <p>Total Versions: {metrics.summary.versions}</p>
      <p>Total Columns: {metrics.summary.columns}</p>

      <h4>Layer Details</h4>
      {metrics.layers.map((l) => (
        <div key={l.layer} style={{ marginBottom: "10px" }}>
          <strong>{l.layer}</strong>
          <p>Tables: {l.tables}</p>
          <p>Versions: {l.versions}</p>
          <p>Columns: {l.columns}</p>
        </div>
      ))}
    </div>
  );
}
