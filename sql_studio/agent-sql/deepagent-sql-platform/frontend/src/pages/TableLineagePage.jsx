import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Link } from "react-router-dom";
import {
  ArrowBack,
  FilterList,
  Refresh,
  Search,
  ZoomIn,
  ZoomOut,
  FitScreen,
  Home
} from "@mui/icons-material";
import {
  Box,
  Button,
  Chip,
  Container,
  Divider,
  InputAdornment,
  IconButton,
  MenuItem,
  Paper,
  Select,
  Stack,
  TextField,
  Typography
} from "@mui/material";
import { fetchTableCanvasLineage } from "../services/api";

const LAYER_COLORS = {
  ODP: { border: "rgba(30,215,96,0.8)", bg: "rgba(30,215,96,0.10)", glow: "rgba(30,215,96,0.25)", text: "#1ed760" },
  FDP: { border: "rgba(11,95,255,0.8)", bg: "rgba(11,95,255,0.10)", glow: "rgba(11,95,255,0.25)", text: "#0b5fff" },
  CDP: { border: "rgba(168,85,247,0.8)", bg: "rgba(168,85,247,0.10)", glow: "rgba(168,85,247,0.25)", text: "#a855f7" }
};

const toNumber = (value) => {
  const n = Number(value);
  return Number.isFinite(n) ? n : 0;
};

const normalizeLineagePayload = (payload) => {
  const rawTables = Array.isArray(payload?.tables)
    ? payload.tables
    : Array.isArray(payload?.nodes)
      ? payload.nodes
      : [];

  const tables = rawTables.map((table, index) => ({
    id: String(table.id ?? table.table_id ?? table.name ?? `table_${index + 1}`),
    name: String(table.name ?? table.label ?? table.table ?? `table_${index + 1}`),
    layer: String(table.layer ?? table.stage ?? "ODP"),
    schema: String(table.schema ?? table.db_schema ?? "-"),
    columns: toNumber(table.columns ?? table.column_count),
    rows: toNumber(table.rows ?? table.row_count)
  }));

  const tableIds = new Set(tables.map((t) => t.id));

  const rawEdges = Array.isArray(payload?.edges)
    ? payload.edges
    : Array.isArray(payload?.links)
      ? payload.links
      : [];

  const edges = rawEdges
    .map((edge) => {
      if (Array.isArray(edge)) {
        return [String(edge[0] ?? ""), String(edge[1] ?? "")];
      }
      return [String(edge?.from ?? edge?.source ?? ""), String(edge?.to ?? edge?.target ?? "")];
    })
    .filter(([from, to]) => from && to && tableIds.has(from) && tableIds.has(to));

  return { tables, edges };
};

export default function TableLineagePage() {
  const canvasRef = useRef(null);
  const nodePositions = useRef(new Map());
  const viewportRef = useRef({ scale: 1, offsetX: 0, offsetY: 0 });
  const interactionRef = useRef({ mode: null, startX: 0, startY: 0, startOffsetX: 0, startOffsetY: 0 });
  const hasAutoFitRef = useRef(false);

  const [refresh, setRefresh] = useState(false);
  const [search, setSearch] = useState("");
  const [layerFilter, setLayerFilter] = useState("all");
  const [selectedTable, setSelectedTable] = useState(null);
  const [hoveredId, setHoveredId] = useState(null);
  const [tables, setTables] = useState([]);
  const [edges, setEdges] = useState([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    let isActive = true;
    setIsLoading(true);
    setError("");

    fetchTableCanvasLineage()
      .then((data) => {
        if (!isActive) return;
        console.log("Table canvas lineage API response:", data);
        const normalized = normalizeLineagePayload(data);
        setTables(normalized.tables);
        setEdges(normalized.edges);
        setIsLoading(false);
      })
      .catch((err) => {
        if (!isActive) return;
        console.error("Table canvas lineage fetch failed", err);
        setTables([]);
        setEdges([]);
        setError("Failed to load table lineage data from /metadata/canvas");
        setIsLoading(false);
      });

    return () => {
      isActive = false;
    };
  }, [refresh]);

  const layers = useMemo(() => {
    const preferred = ["ODP", "FDP", "CDP"];
    const present = [...new Set(tables.map((t) => t.layer))];
    const preferredPresent = preferred.filter((p) => present.includes(p));
    const extra = present.filter((layer) => !preferred.includes(layer));
    return [...preferredPresent, ...extra];
  }, [tables]);

  const tablesById = useMemo(() => new Map(tables.map((t) => [t.id, t])), [tables]);

  const filteredTables = useMemo(
    () =>
      tables.filter((t) => {
        if (layerFilter !== "all" && t.layer !== layerFilter) return false;
        if (search && !t.name.toLowerCase().includes(search.toLowerCase())) return false;
        return true;
      }),
    [tables, layerFilter, search]
  );

  const visibleIds = useMemo(() => new Set(filteredTables.map((t) => t.id)), [filteredTables]);
  const visibleEdges = useMemo(
    () => edges.filter(([from, to]) => visibleIds.has(from) && visibleIds.has(to)),
    [edges, visibleIds]
  );

  const screenToWorld = useCallback((sx, sy) => {
    const { scale, offsetX, offsetY } = viewportRef.current;
    return {
      x: (sx - offsetX) / scale,
      y: (sy - offsetY) / scale
    };
  }, []);

  const fitToContent = useCallback(() => {
    const canvas = canvasRef.current;
    if (!canvas || filteredTables.length === 0) return;

    const rect = canvas.getBoundingClientRect();
    const drawLayers = layers.length ? layers : ["ODP", "FDP", "CDP"];
    const layerGap = 280;
    const nodeW = 170;
    const nodeH = 58;
    const colGap = 24;
    const rowGap = 28;

    let minX = Infinity;
    let minY = Infinity;
    let maxX = -Infinity;
    let maxY = -Infinity;

    drawLayers.forEach((layer, li) => {
      const layerTables = filteredTables.filter((t) => t.layer === layer);
      layerTables.forEach((_, i) => {
        const x = li * (nodeW + layerGap);
        const y = i * (nodeH + rowGap) + colGap;
        minX = Math.min(minX, x);
        minY = Math.min(minY, y);
        maxX = Math.max(maxX, x + nodeW);
        maxY = Math.max(maxY, y + nodeH);
      });
    });

    if (!Number.isFinite(minX)) return;

    const pad = 80;
    const worldW = Math.max(1, maxX - minX + pad * 2);
    const worldH = Math.max(1, maxY - minY + pad * 2);
    const scale = Math.max(0.35, Math.min(1.7, Math.min(rect.width / worldW, rect.height / worldH)));

    viewportRef.current = {
      scale,
      offsetX: rect.width / 2 - (minX + maxX) / 2 * scale,
      offsetY: rect.height / 2 - (minY + maxY) / 2 * scale
    };
  }, [filteredTables, layers]);

  const zoomAt = useCallback((mx, my, delta) => {
    const { scale, offsetX, offsetY } = viewportRef.current;
    const nextScale = Math.max(0.35, Math.min(2.6, scale * delta));
    const worldX = (mx - offsetX) / scale;
    const worldY = (my - offsetY) / scale;
    viewportRef.current = {
      scale: nextScale,
      offsetX: mx - worldX * nextScale,
      offsetY: my - worldY * nextScale
    };
  }, []);

  const drawGraph = useCallback(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    const dpr = window.devicePixelRatio || 1;
    const rect = canvas.getBoundingClientRect();
    canvas.width = Math.max(1, Math.floor(rect.width * dpr));
    canvas.height = Math.max(1, Math.floor(rect.height * dpr));
    ctx.setTransform(1, 0, 0, 1, 0, 0);
    ctx.scale(dpr, dpr);

    const w = rect.width;
    const h = rect.height;
    ctx.clearRect(0, 0, w, h);

    const { scale, offsetX, offsetY } = viewportRef.current;
    const grid = 50;
    const left = (-offsetX) / scale;
    const right = (w - offsetX) / scale;
    const top = (-offsetY) / scale;
    const bottom = (h - offsetY) / scale;

    ctx.save();
    ctx.translate(offsetX, offsetY);
    ctx.scale(scale, scale);

    ctx.strokeStyle = "rgba(255,255,255,0.03)";
    ctx.lineWidth = 1 / scale;
    for (let x = Math.floor(left / grid) * grid; x < right; x += grid) {
      ctx.beginPath();
      ctx.moveTo(x, top);
      ctx.lineTo(x, bottom);
      ctx.stroke();
    }
    for (let y = Math.floor(top / grid) * grid; y < bottom; y += grid) {
      ctx.beginPath();
      ctx.moveTo(left, y);
      ctx.lineTo(right, y);
      ctx.stroke();
    }

    const drawLayers = layers.length ? layers : ["ODP", "FDP", "CDP"];
    const positions = new Map();
    const nodeH = 58;
    const nodeW = 170;
    const layerGap = 280;
    const rowGap = 28;
    const topPad = 36;

    drawLayers.forEach((layer, li) => {
      const layerTables = filteredTables.filter((t) => t.layer === layer);
      const x = li * (nodeW + layerGap);
      layerTables.forEach((table, i) => {
        positions.set(table.id, {
          x,
          y: topPad + i * (nodeH + rowGap),
          w: nodeW,
          h: nodeH
        });
      });
    });

    nodePositions.current = positions;

    drawLayers.forEach((layer, li) => {
      const cx = li * (nodeW + layerGap) + nodeW / 2;
      ctx.fillStyle = "rgba(255,255,255,0.2)";
      ctx.font = "bold 12px 'Poppins', sans-serif";
      ctx.textAlign = "center";
      ctx.fillText(`${layer} Layer`, cx, 16);
    });

    visibleEdges.forEach(([from, to]) => {
      const fp = positions.get(from);
      const tp = positions.get(to);
      if (!fp || !tp) return;

      const isHighlighted =
        hoveredId === from ||
        hoveredId === to ||
        selectedTable?.id === from ||
        selectedTable?.id === to;

      const fromTable = tablesById.get(from);
      const toTable = tablesById.get(to);
      const fromColor = LAYER_COLORS[fromTable?.layer] || LAYER_COLORS.ODP;
      const toColor = LAYER_COLORS[toTable?.layer] || LAYER_COLORS.FDP;
      const gradient = ctx.createLinearGradient(fp.x + fp.w, fp.y + fp.h / 2, tp.x, tp.y + tp.h / 2);
      gradient.addColorStop(0, isHighlighted ? fromColor.border : fromColor.glow);
      gradient.addColorStop(1, isHighlighted ? toColor.border : toColor.glow);

      ctx.strokeStyle = gradient;
      ctx.lineWidth = isHighlighted ? 2.3 : 1.2;
      ctx.beginPath();
      const sx = fp.x + fp.w;
      const sy = fp.y + fp.h / 2;
      const ex = tp.x;
      const ey = tp.y + tp.h / 2;
      const cpx = (sx + ex) / 2;
      ctx.moveTo(sx, sy);
      ctx.bezierCurveTo(cpx, sy, cpx, ey, ex, ey);
      ctx.stroke();

      ctx.fillStyle = isHighlighted ? toColor.border : toColor.glow;
      ctx.beginPath();
      ctx.moveTo(ex, ey);
      ctx.lineTo(ex - 8, ey - 4);
      ctx.lineTo(ex - 8, ey + 4);
      ctx.closePath();
      ctx.fill();
    });

    positions.forEach((pos, id) => {
      const table = tablesById.get(id);
      if (!table) return;

      const colors = LAYER_COLORS[table.layer] || LAYER_COLORS.ODP;
      const isSelected = selectedTable?.id === id;
      const isHovered = hoveredId === id;
      const isActive = isSelected || isHovered;

      const roundRect = () => {
        ctx.beginPath();
        if (typeof ctx.roundRect === "function") {
          ctx.roundRect(pos.x, pos.y, pos.w, pos.h, 10);
        } else {
          const r = 10;
          ctx.moveTo(pos.x + r, pos.y);
          ctx.arcTo(pos.x + pos.w, pos.y, pos.x + pos.w, pos.y + pos.h, r);
          ctx.arcTo(pos.x + pos.w, pos.y + pos.h, pos.x, pos.y + pos.h, r);
          ctx.arcTo(pos.x, pos.y + pos.h, pos.x, pos.y, r);
          ctx.arcTo(pos.x, pos.y, pos.x + pos.w, pos.y, r);
          ctx.closePath();
        }
      };

      ctx.shadowColor = isActive ? colors.glow : "transparent";
      ctx.shadowBlur = isActive ? 20 : 0;
      ctx.fillStyle = isActive ? colors.bg.replace("0.10", "0.18") : colors.bg;
      roundRect();
      ctx.fill();

      ctx.shadowBlur = 0;
      ctx.strokeStyle = isActive ? colors.border : colors.border.replace("0.8", "0.4");
      ctx.lineWidth = isActive ? 2 : 1;
      roundRect();
      ctx.stroke();

      ctx.fillStyle = "rgba(255,255,255,0.92)";
      ctx.font = "600 11px 'JetBrains Mono', monospace";
      ctx.textAlign = "center";
      ctx.textBaseline = "middle";
      ctx.fillText(table.name, pos.x + pos.w / 2, pos.y + 20);

      ctx.fillStyle = "rgba(255,255,255,0.42)";
      ctx.font = "10px 'Poppins', sans-serif";
      ctx.fillText(`${table.columns} cols | ${table.rows.toLocaleString()} rows`, pos.x + pos.w / 2, pos.y + 40);
    });

    ctx.restore();
  }, [filteredTables, hoveredId, layers, selectedTable, tablesById, visibleEdges]);

  useEffect(() => {
    drawGraph();
  }, [drawGraph]);

  useEffect(() => {
    if (!isLoading && filteredTables.length > 0 && !hasAutoFitRef.current) {
      fitToContent();
      hasAutoFitRef.current = true;
      drawGraph();
    }
  }, [drawGraph, filteredTables, fitToContent, isLoading]);

  useEffect(() => {
    const handleResize = () => drawGraph();
    window.addEventListener("resize", handleResize);
    return () => window.removeEventListener("resize", handleResize);
  }, [drawGraph]);

  useEffect(() => {
    if (selectedTable && !visibleIds.has(selectedTable.id)) {
      setSelectedTable(null);
    }
  }, [selectedTable, visibleIds]);

  const handleCanvasClick = (e) => {
    const rect = canvasRef.current?.getBoundingClientRect();
    if (!rect) return;
    const { x: mx, y: my } = screenToWorld(e.clientX - rect.left, e.clientY - rect.top);
    let found = null;
    nodePositions.current.forEach((pos, id) => {
      if (mx >= pos.x && mx <= pos.x + pos.w && my >= pos.y && my <= pos.y + pos.h) {
        found = tablesById.get(id) || null;
      }
    });
    setSelectedTable(found);
  };

  const handleCanvasMouseMove = (e) => {
    const rect = canvasRef.current?.getBoundingClientRect();
    if (!rect) return;
    const sx = e.clientX - rect.left;
    const sy = e.clientY - rect.top;

    if (interactionRef.current.mode === "pan") {
      viewportRef.current.offsetX = interactionRef.current.startOffsetX + (sx - interactionRef.current.startX);
      viewportRef.current.offsetY = interactionRef.current.startOffsetY + (sy - interactionRef.current.startY);
      drawGraph();
      return;
    }

    const { x: mx, y: my } = screenToWorld(sx, sy);
    let found = null;
    nodePositions.current.forEach((pos, id) => {
      if (mx >= pos.x && mx <= pos.x + pos.w && my >= pos.y && my <= pos.y + pos.h) {
        found = id;
      }
    });
    setHoveredId(found);
    if (canvasRef.current) canvasRef.current.style.cursor = found ? "pointer" : "default";
  };

  const handleCanvasMouseDown = (e) => {
    const rect = canvasRef.current?.getBoundingClientRect();
    if (!rect) return;
    const sx = e.clientX - rect.left;
    const sy = e.clientY - rect.top;
    const { x: mx, y: my } = screenToWorld(sx, sy);

    let found = null;
    nodePositions.current.forEach((pos, id) => {
      if (mx >= pos.x && mx <= pos.x + pos.w && my >= pos.y && my <= pos.y + pos.h) {
        found = id;
      }
    });

    if (!found) {
      interactionRef.current = {
        mode: "pan",
        startX: sx,
        startY: sy,
        startOffsetX: viewportRef.current.offsetX,
        startOffsetY: viewportRef.current.offsetY
      };
      if (canvasRef.current) canvasRef.current.style.cursor = "grabbing";
    }
  };

  const handleCanvasMouseUp = () => {
    interactionRef.current.mode = null;
    if (canvasRef.current) {
      canvasRef.current.style.cursor = hoveredId ? "pointer" : "default";
    }
  };

  const handleCanvasWheel = (e) => {
    e.preventDefault();
    const rect = canvasRef.current?.getBoundingClientRect();
    if (!rect) return;
    const sx = e.clientX - rect.left;
    const sy = e.clientY - rect.top;
    zoomAt(sx, sy, e.deltaY < 0 ? 1.12 : 0.9);
    drawGraph();
  };

  const selectedUpstream = selectedTable
    ? visibleEdges
        .filter(([, to]) => to === selectedTable.id)
        .map(([from]) => tablesById.get(from))
        .filter(Boolean)
    : [];

  const selectedDownstream = selectedTable
    ? visibleEdges
        .filter(([from]) => from === selectedTable.id)
        .map(([, to]) => tablesById.get(to))
        .filter(Boolean)
    : [];

  return (
    <Box
      sx={{
        minHeight: "100vh",
        background:
          "radial-gradient(1100px 650px at 8% -10%, rgba(30,215,96,0.12), transparent 60%), radial-gradient(900px 500px at 90% 2%, rgba(11,95,255,0.14), transparent 55%), #0b0f0e",
        color: "#f4f7f6",
        py: 4
      }}
    >
      <Container maxWidth="xl">
        <Box sx={{ display: "flex", alignItems: "center", justifyContent: "space-between", mb: 3 }}>
          <Box sx={{ display: "flex", alignItems: "center", gap: 1.5 }}>
            <IconButton component={Link} to="/" sx={{ color: "#f4f7f6" }} aria-label="Go to landing page">
              <Home />
            </IconButton>
            <IconButton component={Link} to="/app" sx={{ color: "#f4f7f6" }}>
              <ArrowBack />
            </IconButton>
            <Box>
              <Typography variant="h4" sx={{ fontWeight: 800 }}>Table Lineage</Typography>
              <Typography sx={{ color: "rgba(244,247,246,0.72)" }}>
                Trace data flow across ODP, FDP, and CDP tables
              </Typography>
            </Box>
          </Box>
          <Stack direction="row" spacing={1.25}>
            <Button component={Link} to="/lineage/columns" variant="outlined" sx={{ borderColor: "rgba(255,255,255,0.25)", color: "#f4f7f6" }}>
              Column Lineage
            </Button>
            <Button component={Link} to="/app" variant="outlined" sx={{ borderColor: "rgba(255,255,255,0.25)", color: "#f4f7f6" }}>
              Back To Main
            </Button>
          </Stack>
        </Box>

        <Paper
          elevation={0}
          sx={{
            p: 1.5,
            mb: 2,
            display: "flex",
            gap: 1.5,
            alignItems: "center",
            borderRadius: 2,
            bgcolor: "rgba(255,255,255,0.05)",
            border: "1px solid rgba(255,255,255,0.12)"
          }}
        >
          <TextField
            size="small"
            placeholder="Search tables..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            InputProps={{
              startAdornment: (
                <InputAdornment position="start">
                  <Search sx={{ fontSize: 18, opacity: 0.7 }} />
                </InputAdornment>
              )
            }}
            sx={{ minWidth: 260, "& .MuiInputBase-root": { color: "#f4f7f6" } }}
          />
          <Box sx={{ display: "flex", alignItems: "center", gap: 0.75 }}>
            <FilterList sx={{ fontSize: 18, opacity: 0.7 }} />
            <Select
              size="small"
              value={layerFilter}
              onChange={(e) => setLayerFilter(e.target.value)}
              sx={{ minWidth: 160, color: "#f4f7f6" }}
            >
              <MenuItem value="all">All Layers</MenuItem>
              {layers.map((layer) => (
                <MenuItem key={layer} value={layer}>{layer}</MenuItem>
              ))}
            </Select>
          </Box>

          <Stack direction="row" spacing={1} sx={{ ml: "auto" }}>
            <IconButton
              sx={{ color: "#f4f7f6" }}
              onClick={() => {
                const rect = canvasRef.current?.getBoundingClientRect();
                if (!rect) return;
                zoomAt(rect.width / 2, rect.height / 2, 1.12);
                drawGraph();
              }}
            >
              <ZoomIn />
            </IconButton>
            <IconButton
              sx={{ color: "#f4f7f6" }}
              onClick={() => {
                const rect = canvasRef.current?.getBoundingClientRect();
                if (!rect) return;
                zoomAt(rect.width / 2, rect.height / 2, 0.9);
                drawGraph();
              }}
            >
              <ZoomOut />
            </IconButton>
            <IconButton
              sx={{ color: "#f4f7f6" }}
              onClick={() => {
                fitToContent();
                drawGraph();
              }}
            >
              <FitScreen />
            </IconButton>
            <Button
              variant="outlined"
              startIcon={<Refresh />}
              onClick={() => {
                hasAutoFitRef.current = false;
                setRefresh((p) => !p);
              }}
              sx={{ borderColor: "rgba(255,255,255,0.25)", color: "#f4f7f6" }}
            >
              Refresh
            </Button>
          </Stack>
        </Paper>

        <Box sx={{ display: "grid", gridTemplateColumns: selectedTable ? "1fr 300px" : "1fr", gap: 2 }}>
          <Paper
            elevation={0}
            sx={{
              borderRadius: 2,
              bgcolor: "rgba(255,255,255,0.05)",
              border: "1px solid rgba(255,255,255,0.12)",
              overflow: "hidden"
            }}
          >
            <Box sx={{ px: 2, py: 1.25, borderBottom: "1px solid rgba(255,255,255,0.12)", display: "flex", justifyContent: "space-between", alignItems: "center" }}>
              <Box sx={{ display: "flex", alignItems: "center", gap: 1.25 }}>
                <Typography sx={{ fontWeight: 700 }}>Table Flow</Typography>
                <Chip label={`${filteredTables.length} tables`} size="small" sx={{ color: "#1ed760", bgcolor: "rgba(30,215,96,0.15)" }} />
              </Box>
              <Stack direction="row" spacing={1}>
                {layers.map((layer) => {
                  const color = LAYER_COLORS[layer] || LAYER_COLORS.ODP;
                  return (
                    <Chip
                      key={layer}
                      size="small"
                      label={layer}
                      variant="outlined"
                      sx={{ borderColor: color.border, color: color.text }}
                    />
                  );
                })}
              </Stack>
            </Box>
            <Box sx={{ height: "68vh", minHeight: 420, position: "relative" }}>
              <canvas
                ref={canvasRef}
                style={{ display: "block", width: "100%", height: "100%" }}
                onClick={handleCanvasClick}
                onMouseDown={handleCanvasMouseDown}
                onMouseMove={handleCanvasMouseMove}
                onMouseUp={handleCanvasMouseUp}
                onMouseLeave={() => {
                  handleCanvasMouseUp();
                  setHoveredId(null);
                }}
                onWheel={handleCanvasWheel}
              />
              {isLoading && (
                <Box sx={{ position: "absolute", inset: 0, display: "flex", alignItems: "center", justifyContent: "center", bgcolor: "rgba(0,0,0,0.25)" }}>
                  <Typography>Loading table lineage...</Typography>
                </Box>
              )}
              {!isLoading && error && (
                <Box sx={{ position: "absolute", inset: 0, display: "flex", alignItems: "center", justifyContent: "center", bgcolor: "rgba(0,0,0,0.35)", px: 2 }}>
                  <Typography color="error.light" align="center">{error}</Typography>
                </Box>
              )}
            </Box>
          </Paper>

          {selectedTable && (
            <Paper
              elevation={0}
              sx={{
                borderRadius: 2,
                bgcolor: "rgba(255,255,255,0.05)",
                border: "1px solid rgba(255,255,255,0.12)",
                p: 2
              }}
            >
              <Typography sx={{ fontWeight: 700 }}>{selectedTable.name}</Typography>
              <Chip
                size="small"
                label={selectedTable.layer}
                variant="outlined"
                sx={{ mt: 1, borderColor: (LAYER_COLORS[selectedTable.layer] || LAYER_COLORS.ODP).border, color: (LAYER_COLORS[selectedTable.layer] || LAYER_COLORS.ODP).text }}
              />
              <Divider sx={{ my: 2, borderColor: "rgba(255,255,255,0.12)" }} />
              <Stack spacing={1.25}>
                <Box sx={{ display: "flex", justifyContent: "space-between" }}>
                  <Typography sx={{ color: "rgba(255,255,255,0.65)" }}>Schema</Typography>
                  <Typography sx={{ fontFamily: "monospace" }}>{selectedTable.schema}</Typography>
                </Box>
                <Box sx={{ display: "flex", justifyContent: "space-between" }}>
                  <Typography sx={{ color: "rgba(255,255,255,0.65)" }}>Columns</Typography>
                  <Typography sx={{ fontFamily: "monospace" }}>{selectedTable.columns}</Typography>
                </Box>
                <Box sx={{ display: "flex", justifyContent: "space-between" }}>
                  <Typography sx={{ color: "rgba(255,255,255,0.65)" }}>Rows</Typography>
                  <Typography sx={{ fontFamily: "monospace" }}>{selectedTable.rows.toLocaleString()}</Typography>
                </Box>
              </Stack>
              <Divider sx={{ my: 2, borderColor: "rgba(255,255,255,0.12)" }} />

              <Typography sx={{ fontSize: 12, letterSpacing: 1, opacity: 0.7, mb: 1 }}>UPSTREAM</Typography>
              <Stack direction="row" spacing={0.75} useFlexGap sx={{ flexWrap: "wrap", mb: 2 }}>
                {selectedUpstream.length === 0 && <Typography sx={{ fontSize: 12, opacity: 0.7 }}>Source table</Typography>}
                {selectedUpstream.map((table) => (
                  <Chip key={table.id} size="small" label={table.name} />
                ))}
              </Stack>

              <Typography sx={{ fontSize: 12, letterSpacing: 1, opacity: 0.7, mb: 1 }}>DOWNSTREAM</Typography>
              <Stack direction="row" spacing={0.75} useFlexGap sx={{ flexWrap: "wrap" }}>
                {selectedDownstream.length === 0 && <Typography sx={{ fontSize: 12, opacity: 0.7 }}>Terminal table</Typography>}
                {selectedDownstream.map((table) => (
                  <Chip key={table.id} size="small" label={table.name} />
                ))}
              </Stack>
            </Paper>
          )}
        </Box>
      </Container>
    </Box>
  );
}
