import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Link } from "react-router-dom";
import { ArrowBack, ArrowForward, FitScreen, Refresh, Search, ViewColumn, ZoomIn, ZoomOut, Home } from "@mui/icons-material";
import {
  Box,
  Button,
  Chip,
  Container,
  Divider,
  IconButton,
  InputAdornment,
  MenuItem,
  Paper,
  Select,
  Stack,
  TextField,
  Typography
} from "@mui/material";
import { fetchColumnCanvasLineage } from "../services/api";

const LAYER_COLORS = {
  ODP: { border: "rgba(30,215,96,0.8)", bg: "rgba(30,215,96,0.10)", glow: "rgba(30,215,96,0.25)", text: "#1ed760" },
  FDP: { border: "rgba(11,95,255,0.8)", bg: "rgba(11,95,255,0.10)", glow: "rgba(11,95,255,0.25)", text: "#0b5fff" },
  CDP: { border: "rgba(168,85,247,0.8)", bg: "rgba(168,85,247,0.10)", glow: "rgba(168,85,247,0.25)", text: "#a855f7" }
};

const normalizeColumnsPayload = (payload) => {
  const rawColumns = Array.isArray(payload?.columns)
    ? payload.columns
    : Array.isArray(payload?.nodes)
      ? payload.nodes
      : [];

  const columns = rawColumns.map((col, index) => ({
    id: String(col.id ?? col.column_id ?? `${col.table || "table"}.${col.column || col.name || index}`),
    column: String(col.column ?? col.name ?? col.label ?? `col_${index + 1}`),
    table: String(col.table ?? col.table_name ?? "unknown_table"),
    layer: String(col.layer ?? col.stage ?? "ODP"),
    type: String(col.type ?? col.data_type ?? "UNKNOWN")
  }));

  const ids = new Set(columns.map((c) => c.id));

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
    .filter(([from, to]) => from && to && ids.has(from) && ids.has(to));

  return { columns, edges };
};

export default function ColumnLineagePage() {
  const canvasRef = useRef(null);
  const nodePositions = useRef(new Map());
  const viewportRef = useRef({ scale: 1, offsetX: 0, offsetY: 0 });
  const interactionRef = useRef({ mode: null, startX: 0, startY: 0, startOffsetX: 0, startOffsetY: 0 });
  const hasAutoFitRef = useRef(false);

  const [refresh, setRefresh] = useState(false);
  const [search, setSearch] = useState("");
  const [tableFilter, setTableFilter] = useState("all");
  const [selectedCol, setSelectedCol] = useState(null);
  const [hoveredId, setHoveredId] = useState(null);
  const [columns, setColumns] = useState([]);
  const [edges, setEdges] = useState([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    let isActive = true;
    setIsLoading(true);
    setError("");

    fetchColumnCanvasLineage()
      .then((data) => {
        if (!isActive) return;
        console.log("Column canvas lineage API response:", data);
        const normalized = normalizeColumnsPayload(data);
        setColumns(normalized.columns);
        setEdges(normalized.edges);
        setIsLoading(false);
      })
      .catch((err) => {
        if (!isActive) return;
        console.error("Column lineage fetch failed", err);
        setColumns([]);
        setEdges([]);
        setError("Failed to load column lineage data from /metadata/columns");
        setIsLoading(false);
      });

    return () => {
      isActive = false;
    };
  }, [refresh]);

  const layers = useMemo(() => {
    const preferred = ["ODP", "FDP", "CDP"];
    const present = [...new Set(columns.map((c) => c.layer))];
    const preferredPresent = preferred.filter((p) => present.includes(p));
    const extra = present.filter((layer) => !preferred.includes(layer));
    return [...preferredPresent, ...extra];
  }, [columns]);

  const columnsById = useMemo(() => new Map(columns.map((c) => [c.id, c])), [columns]);

  const allTables = useMemo(() => [...new Set(columns.map((c) => c.table))], [columns]);

  const filteredColumns = useMemo(
    () =>
      columns.filter((c) => {
        if (tableFilter !== "all" && c.table !== tableFilter) return false;
        if (search && !`${c.column} ${c.table}`.toLowerCase().includes(search.toLowerCase())) return false;
        return true;
      }),
    [columns, search, tableFilter]
  );

  const visibleIds = useMemo(() => new Set(filteredColumns.map((c) => c.id)), [filteredColumns]);
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
    if (!canvas || filteredColumns.length === 0) return;

    const rect = canvas.getBoundingClientRect();
    const drawLayers = layers.length ? layers : ["ODP", "FDP", "CDP"];
    const layerGap = 320;
    const nodeW = 200;
    const nodeH = 22;
    const tableGap = 20;
    const rowGap = 28;

    let minX = Infinity;
    let minY = Infinity;
    let maxX = -Infinity;
    let maxY = -Infinity;

    drawLayers.forEach((layer, li) => {
      const x = li * layerGap;
      const layerCols = filteredColumns.filter((c) => c.layer === layer);
      const tables = [...new Set(layerCols.map((c) => c.table))];
      let yOffset = 60;
      tables.forEach((table) => {
        const cols = layerCols.filter((c) => c.table === table);
        cols.forEach((_, ci) => {
          const y = yOffset + 22 + ci * rowGap;
          minX = Math.min(minX, x - nodeW / 2);
          minY = Math.min(minY, y);
          maxX = Math.max(maxX, x + nodeW / 2);
          maxY = Math.max(maxY, y + nodeH);
        });
        const groupH = cols.length * rowGap + 30;
        yOffset += groupH + tableGap;
      });
    });

    if (!Number.isFinite(minX)) return;

    const pad = 90;
    const worldW = Math.max(1, maxX - minX + pad * 2);
    const worldH = Math.max(1, maxY - minY + pad * 2);
    const scale = Math.max(0.3, Math.min(1.6, Math.min(rect.width / worldW, rect.height / worldH)));

    viewportRef.current = {
      scale,
      offsetX: rect.width / 2 - (minX + maxX) / 2 * scale,
      offsetY: rect.height / 2 - (minY + maxY) / 2 * scale
    };
  }, [filteredColumns, layers]);

  const zoomAt = useCallback((mx, my, delta) => {
    const { scale, offsetX, offsetY } = viewportRef.current;
    const nextScale = Math.max(0.3, Math.min(2.6, scale * delta));
    const worldX = (mx - offsetX) / scale;
    const worldY = (my - offsetY) / scale;
    viewportRef.current = {
      scale: nextScale,
      offsetX: mx - worldX * nextScale,
      offsetY: my - worldY * nextScale
    };
  }, []);

  const getHighlightedIds = useCallback(() => {
    if (!selectedCol && !hoveredId) return new Set();
    const rootId = selectedCol?.id || hoveredId;
    if (!rootId) return new Set();

    const ids = new Set([rootId]);
    const walkUp = (id) => {
      visibleEdges
        .filter(([, to]) => to === id)
        .forEach(([from]) => {
          if (!ids.has(from)) {
            ids.add(from);
            walkUp(from);
          }
        });
    };
    const walkDown = (id) => {
      visibleEdges
        .filter(([from]) => from === id)
        .forEach(([, to]) => {
          if (!ids.has(to)) {
            ids.add(to);
            walkDown(to);
          }
        });
    };

    walkUp(rootId);
    walkDown(rootId);
    return ids;
  }, [selectedCol, hoveredId, visibleEdges]);

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
    const highlighted = getHighlightedIds();
    const hasHighlight = highlighted.size > 0;

    drawLayers.forEach((layer, li) => {
      const cx = li * 320;
      const layerCols = filteredColumns.filter((c) => c.layer === layer);
      const tables = [...new Set(layerCols.map((c) => c.table))];

      let yOffset = 60;
      tables.forEach((table) => {
        const cols = layerCols.filter((c) => c.table === table);
        const groupH = cols.length * 28 + 30;
        const nodeW = 200;

        const drawRounded = (x, y, ww, hh, r) => {
          ctx.beginPath();
          if (typeof ctx.roundRect === "function") {
            ctx.roundRect(x, y, ww, hh, r);
          } else {
            const rr = Math.min(r, ww / 2, hh / 2);
            ctx.moveTo(x + rr, y);
            ctx.arcTo(x + ww, y, x + ww, y + hh, rr);
            ctx.arcTo(x + ww, y + hh, x, y + hh, rr);
            ctx.arcTo(x, y + hh, x, y, rr);
            ctx.arcTo(x, y, x + ww, y, rr);
            ctx.closePath();
          }
        };

        ctx.fillStyle = "rgba(255,255,255,0.03)";
        drawRounded(cx - nodeW / 2 - 8, yOffset - 8, nodeW + 16, groupH + 8, 8);
        ctx.fill();

        ctx.fillStyle = "rgba(255,255,255,0.35)";
        ctx.font = "bold 10px 'Poppins', sans-serif";
        ctx.textAlign = "center";
        ctx.fillText(table, cx, yOffset + 8);

        cols.forEach((col, ci) => {
          const ny = yOffset + 22 + ci * 28;
          positions.set(col.id, { x: cx - nodeW / 2, y: ny, w: nodeW, h: 22 });
        });

        yOffset += groupH + 20;
      });

      ctx.fillStyle = "rgba(255,255,255,0.18)";
      ctx.font = "bold 12px 'Poppins', sans-serif";
      ctx.textAlign = "center";
      ctx.fillText(`${layer} Layer`, cx, 28);
    });

    nodePositions.current = positions;

    visibleEdges.forEach(([from, to]) => {
      const fp = positions.get(from);
      const tp = positions.get(to);
      if (!fp || !tp) return;

      const isLit = highlighted.has(from) && highlighted.has(to);
      const fromCol = columnsById.get(from);
      const toCol = columnsById.get(to);
      const fc = LAYER_COLORS[fromCol?.layer] || LAYER_COLORS.ODP;
      const tc = LAYER_COLORS[toCol?.layer] || LAYER_COLORS.FDP;

      const gradient = ctx.createLinearGradient(fp.x + fp.w, fp.y + fp.h / 2, tp.x, tp.y + tp.h / 2);
      gradient.addColorStop(0, isLit || !hasHighlight ? fc.border : "rgba(255,255,255,0.07)");
      gradient.addColorStop(1, isLit || !hasHighlight ? tc.border : "rgba(255,255,255,0.07)");
      ctx.strokeStyle = gradient;
      ctx.lineWidth = isLit ? 2 : hasHighlight ? 0.6 : 1;
      ctx.beginPath();
      const sx = fp.x + fp.w;
      const sy = fp.y + fp.h / 2;
      const ex = tp.x;
      const ey = tp.y + tp.h / 2;
      ctx.moveTo(sx, sy);
      ctx.bezierCurveTo((sx + ex) / 2, sy, (sx + ex) / 2, ey, ex, ey);
      ctx.stroke();
    });

    positions.forEach((pos, id) => {
      const col = columnsById.get(id);
      if (!col) return;
      const colors = LAYER_COLORS[col.layer] || LAYER_COLORS.ODP;
      const isLit = !hasHighlight || highlighted.has(id);
      const isActive = selectedCol?.id === id || hoveredId === id;

      const drawRounded = () => {
        ctx.beginPath();
        if (typeof ctx.roundRect === "function") {
          ctx.roundRect(pos.x, pos.y, pos.w, pos.h, 6);
        } else {
          const r = 6;
          ctx.moveTo(pos.x + r, pos.y);
          ctx.arcTo(pos.x + pos.w, pos.y, pos.x + pos.w, pos.y + pos.h, r);
          ctx.arcTo(pos.x + pos.w, pos.y + pos.h, pos.x, pos.y + pos.h, r);
          ctx.arcTo(pos.x, pos.y + pos.h, pos.x, pos.y, r);
          ctx.arcTo(pos.x, pos.y, pos.x + pos.w, pos.y, r);
          ctx.closePath();
        }
      };

      ctx.shadowColor = isActive ? colors.glow : "transparent";
      ctx.shadowBlur = isActive ? 12 : 0;
      ctx.fillStyle = isActive ? colors.bg.replace("0.10", "0.22") : isLit ? colors.bg : "rgba(255,255,255,0.02)";
      drawRounded();
      ctx.fill();

      ctx.shadowBlur = 0;
      ctx.strokeStyle = isActive ? colors.border : isLit ? colors.border.replace("0.8", "0.35") : "rgba(255,255,255,0.08)";
      ctx.lineWidth = isActive ? 1.5 : 0.8;
      drawRounded();
      ctx.stroke();

      ctx.fillStyle = isLit ? "rgba(255,255,255,0.9)" : "rgba(255,255,255,0.25)";
      ctx.font = "500 10px 'JetBrains Mono', monospace";
      ctx.textAlign = "left";
      ctx.textBaseline = "middle";
      ctx.fillText(col.column, pos.x + 8, pos.y + pos.h / 2);

      ctx.fillStyle = isLit ? "rgba(255,255,255,0.45)" : "rgba(255,255,255,0.15)";
      ctx.font = "9px 'JetBrains Mono', monospace";
      ctx.textAlign = "right";
      ctx.fillText(col.type, pos.x + pos.w - 8, pos.y + pos.h / 2);
    });
    ctx.restore();
  }, [columnsById, filteredColumns, getHighlightedIds, hoveredId, layers, selectedCol, visibleEdges]);

  useEffect(() => {
    drawGraph();
  }, [drawGraph]);

  useEffect(() => {
    if (!isLoading && filteredColumns.length > 0 && !hasAutoFitRef.current) {
      fitToContent();
      hasAutoFitRef.current = true;
      drawGraph();
    }
  }, [drawGraph, filteredColumns, fitToContent, isLoading]);

  useEffect(() => {
    const handleResize = () => drawGraph();
    window.addEventListener("resize", handleResize);
    return () => window.removeEventListener("resize", handleResize);
  }, [drawGraph]);

  useEffect(() => {
    if (selectedCol && !visibleIds.has(selectedCol.id)) {
      setSelectedCol(null);
    }
  }, [selectedCol, visibleIds]);

  const handleCanvasClick = (e) => {
    const rect = canvasRef.current?.getBoundingClientRect();
    if (!rect) return;
    const { x: mx, y: my } = screenToWorld(e.clientX - rect.left, e.clientY - rect.top);
    let found = null;
    nodePositions.current.forEach((pos, id) => {
      if (mx >= pos.x && mx <= pos.x + pos.w && my >= pos.y && my <= pos.y + pos.h) {
        found = columnsById.get(id) || null;
      }
    });
    setSelectedCol(found);
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

  const lineageTrail = useMemo(() => {
    if (!selectedCol) return [];
    const trail = [];

    const walkUp = (id) => {
      visibleEdges
        .filter(([, to]) => to === id)
        .forEach(([from]) => {
          const c = columnsById.get(from);
          if (c && !trail.some((t) => t.id === c.id)) {
            trail.unshift(c);
            walkUp(from);
          }
        });
    };

    const walkDown = (id) => {
      visibleEdges
        .filter(([from]) => from === id)
        .forEach(([, to]) => {
          const c = columnsById.get(to);
          if (c && !trail.some((t) => t.id === c.id)) {
            trail.push(c);
            walkDown(to);
          }
        });
    };

    walkUp(selectedCol.id);
    trail.push(selectedCol);
    walkDown(selectedCol.id);
    return trail;
  }, [selectedCol, visibleEdges, columnsById]);

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
            <IconButton component={Link} to="/lineage/tables" sx={{ color: "#f4f7f6" }}>
              <ArrowBack />
            </IconButton>
            <Box>
              <Typography variant="h4" sx={{ fontWeight: 800 }}>Column Lineage</Typography>
              <Typography sx={{ color: "rgba(244,247,246,0.72)" }}>
                Trace column-level transformations across ODP, FDP, and CDP
              </Typography>
            </Box>
          </Box>
          <Button component={Link} to="/lineage/tables" variant="outlined" sx={{ borderColor: "rgba(255,255,255,0.25)", color: "#f4f7f6" }}>
            Table Lineage
          </Button>
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
            placeholder="Search columns..."
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
            <ViewColumn sx={{ fontSize: 18, opacity: 0.7 }} />
            <Select
              size="small"
              value={tableFilter}
              onChange={(e) => setTableFilter(e.target.value)}
              sx={{ minWidth: 220, color: "#f4f7f6" }}
            >
              <MenuItem value="all">All Tables</MenuItem>
              {allTables.map((table) => (
                <MenuItem key={table} value={table}>{table}</MenuItem>
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

        <Box sx={{ display: "grid", gridTemplateColumns: selectedCol ? "1fr 340px" : "1fr", gap: 2 }}>
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
                <Typography sx={{ fontWeight: 700 }}>Column Flow</Typography>
                <Chip label={`${filteredColumns.length} columns`} size="small" sx={{ color: "#1ed760", bgcolor: "rgba(30,215,96,0.15)" }} />
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
                  <Typography>Loading column lineage...</Typography>
                </Box>
              )}
              {!isLoading && error && (
                <Box sx={{ position: "absolute", inset: 0, display: "flex", alignItems: "center", justifyContent: "center", bgcolor: "rgba(0,0,0,0.35)", px: 2 }}>
                  <Typography color="error.light" align="center">{error}</Typography>
                </Box>
              )}
            </Box>
          </Paper>

          {selectedCol && (
            <Paper
              elevation={0}
              sx={{
                borderRadius: 2,
                bgcolor: "rgba(255,255,255,0.05)",
                border: "1px solid rgba(255,255,255,0.12)",
                p: 2,
                color: "#eaf2ef",
                "& .MuiTypography-root": {
                  color: "#eaf2ef"
                }
              }}
            >
              <Typography sx={{ fontWeight: 700, fontFamily: "monospace" }}>{selectedCol.column}</Typography>
              <Stack direction="row" spacing={1} sx={{ mt: 1, mb: 1.5 }}>
                <Chip
                  size="small"
                  label={selectedCol.layer}
                  variant="outlined"
                  sx={{ borderColor: (LAYER_COLORS[selectedCol.layer] || LAYER_COLORS.ODP).border, color: (LAYER_COLORS[selectedCol.layer] || LAYER_COLORS.ODP).text }}
                />
                <Typography sx={{ fontSize: 12, opacity: 0.75, alignSelf: "center" }}>{selectedCol.table}</Typography>
              </Stack>
              <Divider sx={{ my: 1.5, borderColor: "rgba(255,255,255,0.12)" }} />
              <Stack spacing={1.25}>
                <Box sx={{ display: "flex", justifyContent: "space-between" }}>
                  <Typography sx={{ color: "rgba(255,255,255,0.65)" }}>Type</Typography>
                  <Typography sx={{ fontFamily: "monospace" }}>{selectedCol.type}</Typography>
                </Box>
                <Box sx={{ display: "flex", justifyContent: "space-between" }}>
                  <Typography sx={{ color: "rgba(255,255,255,0.65)" }}>Table</Typography>
                  <Typography sx={{ fontFamily: "monospace" }}>{selectedCol.table}</Typography>
                </Box>
              </Stack>
              <Divider sx={{ my: 1.5, borderColor: "rgba(255,255,255,0.12)" }} />
              <Typography sx={{ fontSize: 12, letterSpacing: 1, opacity: 0.7, mb: 1 }}>LINEAGE TRAIL</Typography>
              <Stack spacing={1}>
                {lineageTrail.map((col, i) => (
                  <Box key={col.id} sx={{ display: "flex", alignItems: "center", gap: 1 }}>
                    <Chip
                      size="small"
                      label={col.column}
                      color={col.id === selectedCol.id ? "primary" : "default"}
                      sx={{
                        fontFamily: "monospace",
                        ...(col.id !== selectedCol.id
                          ? {
                              color: "#eaf2ef",
                              bgcolor: "rgba(255,255,255,0.14)",
                              borderColor: "rgba(255,255,255,0.24)",
                            }
                          : {})
                      }}
                    />
                    <Typography sx={{ fontSize: 12, opacity: 0.72 }}>({col.table})</Typography>
                    {i < lineageTrail.length - 1 && (
                      <ArrowForward sx={{ fontSize: 14, opacity: 0.7, ml: "auto", color: "#b8c8c2" }} />
                    )}
                  </Box>
                ))}
              </Stack>
            </Paper>
          )}
        </Box>
      </Container>
    </Box>
  );
}
