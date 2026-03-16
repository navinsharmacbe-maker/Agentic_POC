import { useEffect, useRef } from "react";
import { Network } from "vis-network";

export default function VisGraph({ data, focusNodeId = null, onNodeSelect }) {
  const containerRef = useRef(null);
  const networkRef = useRef(null);
  const onNodeSelectRef = useRef(onNodeSelect);
  const resetStylesRef = useRef(() => {});
  const highlightConnectedRef = useRef(() => {});

  useEffect(() => {
    onNodeSelectRef.current = onNodeSelect;
  }, [onNodeSelect]);

  useEffect(() => {
    if (!data || !data.nodes || !containerRef.current) return;

    const groupColors = {
      ODP: "#1976d2",
      FDP: "#2e7d32",
      CDP: "#ed6c02",
      Table: "#6366f1",
      Column: "#06b6d4"
    };

    const preparedNodes = (data.nodes || []).map((node) => {
      const fallback = groupColors[node.group] || "#64748b";
      return {
        ...node,
        color:
          node.color ||
          {
            background: `${fallback}22`,
            border: fallback
          },
        font: node.font || { color: "#e2e8f0" }
      };
    });

    const preparedEdges = (data.edges || []).map((edge, idx) => ({
      ...edge,
      id: edge.id ?? `e_${idx}_${edge.from}_${edge.to}`,
      color: edge.color || "rgba(148,163,184,0.45)",
      width: edge.width || 1.2
    }));

    const adjacency = new Map();
    const outgoing = new Map();
    const incoming = new Map();

    preparedNodes.forEach((n) => {
      const id = String(n.id);
      adjacency.set(id, new Set());
      outgoing.set(id, new Set());
      incoming.set(id, new Set());
    });

    preparedEdges.forEach((e) => {
      const from = String(e.from);
      const to = String(e.to);

      if (!adjacency.has(from)) adjacency.set(from, new Set());
      if (!adjacency.has(to)) adjacency.set(to, new Set());
      if (!outgoing.has(from)) outgoing.set(from, new Set());
      if (!outgoing.has(to)) outgoing.set(to, new Set());
      if (!incoming.has(from)) incoming.set(from, new Set());
      if (!incoming.has(to)) incoming.set(to, new Set());

      adjacency.get(from).add(to);
      adjacency.get(to).add(from);
      outgoing.get(from).add(to);
      incoming.get(to).add(from);
    });

    const baseNodesById = new Map(preparedNodes.map((n) => [String(n.id), { ...n }]));
    const baseEdgesById = new Map(preparedEdges.map((e) => [String(e.id), { ...e }]));

    const options = {
      layout: { improvedLayout: true },
      physics: {
        enabled: true,
        stabilization: {
          enabled: false
        },
        barnesHut: { gravitationalConstant: -30000 }
      },
      groups: {
        ODP: { color: "#1976d2" },
        FDP: { color: "#2e7d32" },
        CDP: { color: "#ed6c02" },
        Table: { shape: "box" },
        Column: { shape: "ellipse" }
      },
      edges: {
        arrows: { to: { enabled: true } },
        font: { align: "middle" },
        smooth: { type: "dynamic" }
      }
    };

    const network = new Network(containerRef.current, { nodes: preparedNodes, edges: preparedEdges }, options);
    networkRef.current = network;

    const nodeDs = network.body.data.nodes;
    const edgeDs = network.body.data.edges;

    const resetStyles = () => {
      nodeDs.update(Array.from(baseNodesById.values()));
      edgeDs.update(Array.from(baseEdgesById.values()));
      network.startSimulation();
    };

    const collectConnected = (startId) => {
      const visited = new Set();
      const queue = [String(startId)];
      while (queue.length) {
        const current = queue.shift();
        if (visited.has(current)) continue;
        visited.add(current);
        const neighbors = adjacency.get(current) || new Set();
        neighbors.forEach((next) => {
          if (!visited.has(next)) queue.push(next);
        });
      }
      return visited;
    };

    const collectDirected = (startId, edgeMap) => {
      const visited = new Set();
      const queue = [String(startId)];
      while (queue.length) {
        const current = queue.shift();
        const neighbors = edgeMap.get(current) || new Set();
        neighbors.forEach((next) => {
          if (!visited.has(next)) {
            visited.add(next);
            queue.push(next);
          }
        });
      }
      return visited;
    };

    const highlightConnected = (selectedNodeId) => {
      const selected = String(selectedNodeId);
      const connected = collectConnected(selected);
      const upstream = collectDirected(selected, incoming);
      const downstream = collectDirected(selected, outgoing);

      const nodeUpdates = [];
      baseNodesById.forEach((base, id) => {
        if (id === selected) {
          nodeUpdates.push({
            id,
            color: {
              background: "rgba(250,204,21,0.28)",
              border: "rgba(250,204,21,0.95)"
            },
            font: { ...(base.font || {}), color: "#fef9c3" }
          });
        } else if (downstream.has(id)) {
          nodeUpdates.push({
            id,
            color: {
              background: "rgba(56,189,248,0.22)",
              border: "rgba(56,189,248,0.95)"
            },
            font: { ...(base.font || {}), color: "#e0f2fe" }
          });
        } else if (upstream.has(id)) {
          nodeUpdates.push({
            id,
            color: {
              background: "rgba(74,222,128,0.22)",
              border: "rgba(74,222,128,0.95)"
            },
            font: { ...(base.font || {}), color: "#dcfce7" }
          });
        } else if (connected.has(id)) {
          nodeUpdates.push({ id, color: base.color, font: base.font });
        } else {
          nodeUpdates.push({
            id,
            color: {
              background: "rgba(2,6,23,0.88)",
              border: "rgba(30,41,59,0.95)",
              highlight: { background: "rgba(2,6,23,0.88)", border: "rgba(30,41,59,0.95)" },
              hover: { background: "rgba(2,6,23,0.88)", border: "rgba(30,41,59,0.95)" }
            },
            font: { ...(base.font || {}), color: "rgba(100,116,139,0.32)" }
          });
        }
      });

      const edgeUpdates = [];
      baseEdgesById.forEach((base, id) => {
        const from = String(base.from);
        const to = String(base.to);
        const inDownstreamPath =
          (from === selected && downstream.has(to)) ||
          (downstream.has(from) && downstream.has(to));
        const inUpstreamPath =
          (to === selected && upstream.has(from)) ||
          (upstream.has(from) && upstream.has(to));

        if (inDownstreamPath) {
          edgeUpdates.push({ id, color: "rgba(56,189,248,0.92)", width: 2.4 });
        } else if (inUpstreamPath) {
          edgeUpdates.push({ id, color: "rgba(74,222,128,0.92)", width: 2.4 });
        } else if (connected.has(from) && connected.has(to)) {
          edgeUpdates.push({ id, color: base.color, width: 1.8 });
        } else {
          edgeUpdates.push({ id, color: "rgba(15,23,42,0.92)", width: 0.35 });
        }
      });

      nodeDs.update(nodeUpdates);
      edgeDs.update(edgeUpdates);
      network.startSimulation();
    };

    resetStylesRef.current = resetStyles;
    highlightConnectedRef.current = highlightConnected;

    network.on("click", (params) => {
      const selectedId = params?.nodes?.[0];
      if (selectedId !== undefined && selectedId !== null) {
        highlightConnectedRef.current(selectedId);
        onNodeSelectRef.current?.(String(selectedId));
      } else {
        resetStylesRef.current();
        onNodeSelectRef.current?.(null);
      }
    });

    return () => {
      network.destroy();
      networkRef.current = null;
    };
  }, [data]);

  useEffect(() => {
    const network = networkRef.current;
    if (!network) return;

    if (focusNodeId) {
      highlightConnectedRef.current(focusNodeId);
      network.selectNodes([focusNodeId]);
    } else {
      resetStylesRef.current();
      network.unselectAll();
    }
  }, [focusNodeId]);

  return <div ref={containerRef} style={{ height: "100%", width: "100%" }} />;
}
