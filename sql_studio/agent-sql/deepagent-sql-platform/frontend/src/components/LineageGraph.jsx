import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import VisGraph from "./VisGraph";
import {
  fetchLineageFullGraph,
  fetchLineageLayerExpand,
  fetchLineageRoots,
  fetchLineageTableExpand,
} from "../services/api";

export default function LineageGraph({
  refresh,
  focusNodeId,
  onGraphLoaded,
  onNodeSelect,
  viewMode = "lazy",
}) {
  const [graph, setGraph] = useState(null);
  const [hasError, setHasError] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const expandedLayersRef = useRef(new Set());
  const expandedTablesRef = useRef(new Set());
  const pendingExpansionsRef = useRef(new Set());

  const nodeById = useMemo(() => {
    const map = new Map();
    (graph?.nodes || []).forEach((node) => map.set(String(node.id), node));
    return map;
  }, [graph]);

  const normalizeNode = useCallback((node) => {
    const id = String(node.id);
    const explicitGroup = String(node.group || "").trim();
    const type = String(node.type || explicitGroup || "").toLowerCase();
    const layer = String(node.layer || "").toUpperCase();
    const group =
      explicitGroup ||
      (type === "layer" ? id : type === "column" ? "Column" : (layer || "Table"));
    return {
      ...node,
      id,
      label: String(node.label ?? node.name ?? id),
      group,
      type: type || "table",
      layer: layer || node.layer || null,
    };
  }, []);

  const mergeGraphData = useCallback((currentGraph, incomingGraph) => {
    const currentNodes = Array.isArray(currentGraph?.nodes) ? currentGraph.nodes : [];
    const currentEdges = Array.isArray(currentGraph?.edges) ? currentGraph.edges : [];
    const incomingNodes = Array.isArray(incomingGraph?.nodes) ? incomingGraph.nodes : [];
    const incomingEdges = Array.isArray(incomingGraph?.edges) ? incomingGraph.edges : [];

    const nodeMap = new Map();
    for (const node of currentNodes) {
      nodeMap.set(String(node.id), normalizeNode(node));
    }
    for (const node of incomingNodes) {
      const normalized = normalizeNode(node);
      const existing = nodeMap.get(normalized.id);
      if (existing) {
        nodeMap.set(normalized.id, {
          ...existing,
          ...normalized,
          stub: Boolean(existing.stub) && Boolean(normalized.stub),
        });
      } else {
        nodeMap.set(normalized.id, normalized);
      }
    }

    const edgeMap = new Map();
    const toEdge = (edge, idx) => {
      const from = String(edge.from);
      const to = String(edge.to);
      const edgeType = String(edge.type || edge.label || "lineage");
      const key = `${from}|${to}|${edgeType}`;
      return {
        id: edge.id ?? `e_${idx}_${key}`,
        from,
        to,
        label: edge.label || "",
        type: edgeType,
      };
    };

    [...currentEdges, ...incomingEdges].forEach((edge, idx) => {
      const normalized = toEdge(edge, idx);
      edgeMap.set(`${normalized.from}|${normalized.to}|${normalized.type}`, normalized);
    });

    return {
      nodes: Array.from(nodeMap.values()),
      edges: Array.from(edgeMap.values()),
    };
  }, [normalizeNode]);

  const loadRoots = useCallback(async () => {
    setHasError(false);
    setIsLoading(true);
    expandedLayersRef.current = new Set();
    expandedTablesRef.current = new Set();
    pendingExpansionsRef.current = new Set();

    try {
      const data = await fetchLineageRoots();
      const rootGraph = mergeGraphData({ nodes: [], edges: [] }, data);
      setGraph(rootGraph);
    } catch (err) {
      console.error("Lineage roots API error:", err);
      setHasError(true);
      setGraph(null);
    } finally {
      setIsLoading(false);
    }
  }, [mergeGraphData]);

  const loadFullGraph = useCallback(async () => {
    setHasError(false);
    setIsLoading(true);
    expandedLayersRef.current = new Set();
    expandedTablesRef.current = new Set();
    pendingExpansionsRef.current = new Set();

    try {
      const data = await fetchLineageFullGraph();
      const fullGraph = mergeGraphData({ nodes: [], edges: [] }, data);
      setGraph(fullGraph);
    } catch (err) {
      console.error("Lineage full graph API error:", err);
      setHasError(true);
      setGraph(null);
    } finally {
      setIsLoading(false);
    }
  }, [mergeGraphData]);

  const expandLayer = useCallback(async (layerId) => {
    const key = `layer:${layerId}`;
    if (expandedLayersRef.current.has(layerId) || pendingExpansionsRef.current.has(key)) return;
    pendingExpansionsRef.current.add(key);
    try {
      const data = await fetchLineageLayerExpand(layerId, true);
      setGraph((prev) => mergeGraphData(prev, data));
      expandedLayersRef.current.add(layerId);
    } catch (err) {
      console.error("Layer expansion failed:", err);
      setHasError(true);
    } finally {
      pendingExpansionsRef.current.delete(key);
    }
  }, [mergeGraphData]);

  const expandTable = useCallback(async (tableId) => {
    const key = `table:${tableId}`;
    if (expandedTablesRef.current.has(tableId) || pendingExpansionsRef.current.has(key)) return;
    pendingExpansionsRef.current.add(key);
    try {
      const data = await fetchLineageTableExpand(tableId);
      setGraph((prev) => mergeGraphData(prev, data));
      expandedTablesRef.current.add(tableId);
    } catch (err) {
      console.error("Table expansion failed:", err);
      setHasError(true);
    } finally {
      pendingExpansionsRef.current.delete(key);
    }
  }, [mergeGraphData]);

  useEffect(() => {
    if (viewMode === "full") {
      loadFullGraph();
      return;
    }
    loadRoots();
  }, [refresh, viewMode, loadRoots, loadFullGraph]);

  useEffect(() => {
    if (graph) onGraphLoaded?.(graph);
  }, [graph, onGraphLoaded]);

  const handleNodeClick = useCallback((nodeId) => {
    onNodeSelect?.(nodeId);
    if (!nodeId) return;

    // Full view is static heavy graph: click should only highlight/focus,
    // not trigger incremental API fetches that reset visual state.
    if (viewMode === "full") return;

    const selected = nodeById.get(String(nodeId));
    if (!selected) return;

    const type = String(selected.type || "").toLowerCase();
    if (type === "layer") {
      expandLayer(String(selected.id).toUpperCase());
      return;
    }
    if (type === "table") {
      expandTable(String(selected.id));
    }
  }, [expandLayer, expandTable, nodeById, onNodeSelect, viewMode]);

  if (!graph) {
    return (
      <div style={{ color: "inherit", opacity: 0.75, padding: "8px 2px" }}>
        {hasError ? "Graph fetch failed." : (isLoading ? "Loading graph..." : "No graph data.")}
      </div>
    );
  }

  return <VisGraph data={graph} focusNodeId={focusNodeId} onNodeSelect={handleNodeClick} />;
}
