import React, { useCallback, useEffect, useMemo, useState } from "react";
import {
  Container,
  Typography,
  Box,
  Paper,
  Stack,
  Divider,
  Button,
  Autocomplete,
  TextField,
  CssBaseline,
  IconButton,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogContentText,
  DialogActions,
  ThemeProvider,
  createTheme
} from "@mui/material";
import {
  Brightness4,
  Brightness7,
  Refresh,
  DeleteForever,
  ZoomIn,
  ZoomOut,
  FitScreen,
  Home
} from "@mui/icons-material";
import { Link } from "react-router-dom";
import MetadataIngestDialog from "../components/MetadataIngestDialog";
import LineageGraph from "../components/LineageGraph";
import LineageGraphPlaceholder from "../components/LineageGraphPlaceholder";
import MetricCard from "../components/MetricCard";
import { dropAllMetadataGraph, fetchGraphMetrics } from "../services/api";

export default function MainPage() {
  const [openIngest, setOpenIngest] = useState(false);
  const [refreshGraph, setRefreshGraph] = useState(false);
  const [mode, setMode] = useState("dark");
  const [confirmDropOpen, setConfirmDropOpen] = useState(false);
  const [dropLoading, setDropLoading] = useState(false);
  const [lineageNodes, setLineageNodes] = useState([]);
  const [selectedLineageNode, setSelectedLineageNode] = useState(null);
  const [graphViewMode, setGraphViewMode] = useState("lazy");
  const [metrics, setMetrics] = useState({
    layers: [],
    summary: { layers: 0, tables: 0, versions: 0, columns: 0 }
  });

  useEffect(() => {
    let isActive = true;
    const loadMetrics = async () => {
      try {
        const res = await fetchGraphMetrics();
        if (isActive) setMetrics(res.data);
      } catch (err) {
        console.error("Graph metrics fetch failed", err);
      }
    };

    loadMetrics();
    const id = setInterval(loadMetrics, 5000);
    return () => {
      isActive = false;
      clearInterval(id);
    };
  }, []);

  const findLayer = (layerName) =>
    metrics.layers.find(l => l.layer === layerName) || {
      tables: 0,
      versions: 0,
      columns: 0
    };

  const odp = findLayer("ODP");
  const fdp = findLayer("FDP");
  const cdp = findLayer("CDP");

  const handleDropAll = async () => {
    try {
      setDropLoading(true);
      await dropAllMetadataGraph();
      setConfirmDropOpen(false);
      setRefreshGraph(p => !p);
      setMetrics({
        layers: [],
        summary: { layers: 0, tables: 0, versions: 0, columns: 0 }
      });
    } catch (err) {
      console.error("Drop graph failed", err);
      alert("Failed to drop Neo4j graph");
    } finally {
      setDropLoading(false);
    }
  };

  const handleGraphLoaded = useCallback((graphData) => {
    const rawNodes = Array.isArray(graphData?.nodes) ? graphData.nodes : [];
    const options = rawNodes
      .map((node, idx) => ({
        id: String(node.id ?? `node_${idx}`),
        label: String(node.label ?? node.name ?? node.id ?? `Node ${idx + 1}`),
      }))
      .sort((a, b) => a.label.localeCompare(b.label));

    setLineageNodes(options);
  }, []);

  const handleGraphNodeSelect = useCallback((nodeId) => {
    if (!nodeId) {
      setSelectedLineageNode(null);
      return;
    }
    const found = lineageNodes.find((node) => node.id === String(nodeId));
    if (found) {
      setSelectedLineageNode(found);
    } else {
      setSelectedLineageNode({ id: String(nodeId), label: String(nodeId) });
    }
  }, [lineageNodes]);

  const sendCanvasControl = (eventName) => {
    window.dispatchEvent(new Event(eventName));
  };

  const theme = useMemo(
    () =>
      createTheme({
        palette: {
          mode,
          primary: { main: mode === "dark" ? "#1ed760" : "#0b5fff" },
          secondary: { main: mode === "dark" ? "#0b5fff" : "#1ed760" },
          background: {
            default: mode === "dark" ? "#0b0f0e" : "#f6f9ff",
            paper: mode === "dark" ? "#111716" : "#ffffff"
          },
          text: {
            primary: mode === "dark" ? "#f4f7f6" : "#0f1a2b",
            secondary: mode === "dark" ? "#b8c2bd" : "#51607a"
          }
        },
        shape: { borderRadius: 14 },
        typography: {
          fontFamily:
            '"Poppins","Montserrat","Segoe UI","Helvetica Neue",Arial,sans-serif'
        }
      }),
    [mode]
  );

  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <Box
        sx={{
          minHeight: "100vh",
          background:
            mode === "dark"
              ? "radial-gradient(1000px 600px at 20% -10%, rgba(30,215,96,0.15), transparent 60%), radial-gradient(900px 500px at 80% 10%, rgba(11,95,255,0.12), transparent 55%), #0b0f0e"
              : "radial-gradient(900px 500px at 15% -10%, rgba(11,95,255,0.12), transparent 60%), radial-gradient(900px 500px at 85% 10%, rgba(30,215,96,0.12), transparent 55%), #f6f9ff"
        }}
      >
        <Container maxWidth="xl" sx={{ py: 5 }}>
          <Box
            sx={{
              display: "flex",
              alignItems: "center",
              justifyContent: "space-between",
              mb: 4
            }}
          >
            <Box sx={{ display: "flex", alignItems: "center", gap: 1.5 }}>
              <IconButton component={Link} to="/" color="primary" aria-label="Go to landing page">
                <Home />
              </IconButton>
              <Box>
                <Typography
                  variant="h3"
                  sx={{ fontWeight: 800, letterSpacing: 0.3 }}
                >
                  Data Platform Metadata and Lineage
                </Typography>
                <Typography sx={{ mt: 1, color: "text.secondary" }}>
                  ODP to FDP to CDP | Tables | Columns | Versions | Lineage
                </Typography>
              </Box>
            </Box>
            <Box sx={{ display: "flex", alignItems: "center", gap: 2 }}>
              <Button
                component={Link}
                to="/sql"
                variant="outlined"
                color="secondary"
              >
                SQL Generation
              </Button>
              <Button
                component={Link}
                to="/mapping"
                variant="outlined"
                color="secondary"
              >
                Mapping Builder
              </Button>
              <Button
                component={Link}
                to="/lineage/tables"
                variant="outlined"
                color="primary"
              >
                Table Lineage
              </Button>

               <Button
                  variant="contained"
                  onClick={() => setOpenIngest(true)}
                >
                  Upload Metadata
                </Button>
              <Button
                variant="outlined"
                color="error"
                startIcon={<DeleteForever />}
                onClick={() => setConfirmDropOpen(true)}
              >
                Drop Database
              </Button>

                <MetadataIngestDialog
                  open={openIngest}
                  onClose={() => setOpenIngest(false)}
                  onSuccess={() => setRefreshGraph(p => !p)}
                />

              <Box
                sx={{
                  display: "flex",
                  alignItems: "center",
                  gap: 1.5,
                  px: 2,
                  py: 1,
                  borderRadius: 999,
                  bgcolor: "background.paper",
                  border: "1px solid",
                  borderColor:
                    mode === "dark" ? "rgba(255,255,255,0.12)" : "rgba(15,23,42,0.12)",
                  boxShadow: mode === "dark" ? "0 8px 16px rgba(0,0,0,0.28)" : "0 3px 10px rgba(15,23,42,0.08)"
                }}
              >
                <Typography sx={{ fontWeight: 600 }}>
                  {mode === "dark" ? "Dark" : "Light"} Mode
                </Typography>
                <IconButton
                  onClick={() =>
                    setMode(prev => (prev === "dark" ? "light" : "dark"))
                  }
                  color="primary"
                >
                  {mode === "dark" ? <Brightness7 /> : <Brightness4 />}
                </IconButton>
              </Box>
            </Box>
          </Box>

          <Stack spacing={3}>
            <Box
              sx={{
                display: "grid",
                gridTemplateColumns: "repeat(4, minmax(0, 1fr))",
                gap: 2
              }}
            >
              <MetricCard
                title="Summary"
                index={0}
                metrics={[
                  { label: "Total Layers", value: metrics.summary.layers },
                  { label: "Total Tables", value: metrics.summary.tables },
                  { label: "Total Versions", value: metrics.summary.versions },
                  { label: "Total Columns", value: metrics.summary.columns }
                ]}
              />

              <MetricCard
                title="ODP"
                index={1}
                metrics={[
                  { label: "Tables", value: odp.tables },
                  { label: "Versions", value: odp.versions },
                  { label: "Columns", value: odp.columns }
                ]}
              />

              <MetricCard
                title="FDP"
                index={2}
                metrics={[
                  { label: "Tables", value: fdp.tables },
                  { label: "Versions", value: fdp.versions },
                  { label: "Columns", value: fdp.columns }
                ]}
              />

              <MetricCard
                title="CDP"
                index={3}
                metrics={[
                  { label: "Tables", value: cdp.tables },
                  { label: "Versions", value: cdp.versions },
                  { label: "Columns", value: cdp.columns }
                ]}
              />
            </Box>

            <Paper
              elevation={0}
              sx={{
                p: 2,
                borderRadius: 2,
                bgcolor: "background.paper",
                border: "1px solid",
                borderColor: mode === "dark" ? "rgba(255,255,255,0.12)" : "rgba(15,23,42,0.12)",
                boxShadow: mode === "dark" ? "0 10px 24px rgba(0,0,0,0.35)" : "0 6px 18px rgba(15,23,42,0.08)",
                height: "75vh",
                maxHeight: "75vh",
                display: "flex",
                flexDirection: "column",
                overflow: "hidden",
                mt: 2
              }}
            >
              <Box
                sx={{
                  display: "flex",
                  alignItems: "center",
                  justifyContent: "space-between",
                  mb: 1
                }}
              >
                <Typography variant="h6" sx={{ fontWeight: 700 }}>
                  Neo4j Lineage Graph
                </Typography>
                <Box sx={{ display: "flex", alignItems: "center", gap: 1.5 }}>
                  <Autocomplete
                    size="small"
                    options={lineageNodes}
                    value={selectedLineageNode}
                    onChange={(_, value) => setSelectedLineageNode(value)}
                    getOptionLabel={(option) => option.label || ""}
                    isOptionEqualToValue={(option, value) => option.id === value.id}
                    sx={{ minWidth: 300 }}
                    renderInput={(params) => (
                      <TextField
                        {...params}
                        placeholder="Search node to isolate lineage..."
                      />
                    )}
                  />
                  <Box
                    sx={{
                      px: 1.5,
                      py: 0.5,
                      borderRadius: 999,
                      bgcolor:
                        mode === "dark"
                          ? "rgba(30,215,96,0.15)"
                          : "rgba(11,95,255,0.12)",
                      color: "primary.main",
                      fontWeight: 600,
                      fontSize: 12
                    }}
                  >
                    {graphViewMode === "full" ? "Full View" : "Lazy View"}
                  </Box>
                  <Button
                    variant={graphViewMode === "lazy" ? "contained" : "outlined"}
                    size="small"
                    onClick={() => {
                      setGraphViewMode("lazy");
                      setRefreshGraph(p => !p);
                    }}
                  >
                    Progressive
                  </Button>
                  <Button
                    variant={graphViewMode === "full" ? "contained" : "outlined"}
                    size="small"
                    color="secondary"
                    onClick={() => {
                      setGraphViewMode("full");
                      setRefreshGraph(p => !p);
                    }}
                  >
                    Full View
                  </Button>
                  <Button
                    variant="contained"
                    size="small"
                    startIcon={<Refresh />}
                    onClick={() => setRefreshGraph(p => !p)}
                  >
                    Refresh
                  </Button>
                </Box>
              </Box>

              <Divider sx={{ mb: 2 }} />

              <Box sx={{ flex: 1, minHeight: 0, height: "100%" }}>
                <LineageGraph
                  refresh={refreshGraph}
                  viewMode={graphViewMode}
                  focusNodeId={selectedLineageNode?.id || null}
                  onGraphLoaded={handleGraphLoaded}
                  onNodeSelect={handleGraphNodeSelect}
                />
              </Box>
            </Paper>

            <Paper
              elevation={0}
              sx={{
                p: 2,
                borderRadius: 2,
                bgcolor: "background.paper",
                border: "1px solid",
                borderColor: mode === "dark" ? "rgba(255,255,255,0.12)" : "rgba(15,23,42,0.12)",
                boxShadow: mode === "dark" ? "0 10px 24px rgba(0,0,0,0.35)" : "0 6px 18px rgba(15,23,42,0.08)",
                height: "56vh",
                minHeight: 460,
                display: "flex",
                flexDirection: "column",
                overflow: "hidden"
              }}
            >
              <Box
                sx={{
                  display: "flex",
                  alignItems: "center",
                  justifyContent: "space-between",
                  mb: 1
                }}
              >
                <Typography variant="h6" sx={{ fontWeight: 700 }}>
                  Canvas Lineage Graph
                </Typography>
                <Typography sx={{ color: "text.secondary", fontSize: 12, mr: "auto", ml: 2 }}>
                  Pan and zoom enabled for large lineage sets
                </Typography>
                <IconButton
                  size="small"
                  sx={{ mr: 0.5 }}
                  onClick={() => sendCanvasControl("canvas-lineage-zoom-in")}
                >
                  <ZoomIn fontSize="small" />
                </IconButton>
                <IconButton
                  size="small"
                  sx={{ mr: 0.5 }}
                  onClick={() => sendCanvasControl("canvas-lineage-zoom-out")}
                >
                  <ZoomOut fontSize="small" />
                </IconButton>
                <IconButton
                  size="small"
                  sx={{ mr: 1 }}
                  onClick={() => sendCanvasControl("canvas-lineage-fit")}
                >
                  <FitScreen fontSize="small" />
                </IconButton>
                <Button
                  variant="outlined"
                  size="small"
                  startIcon={<Refresh />}
                  onClick={() => setRefreshGraph(p => !p)}
                >
                  Refresh
                </Button>
              </Box>

              <Divider sx={{ mb: 2 }} />

              <Box sx={{ flex: 1, minHeight: 0, height: "100%" }}>
                <LineageGraphPlaceholder refresh={refreshGraph} />
              </Box>
            </Paper>

          </Stack>
        </Container>
      </Box>

      <Dialog
        open={confirmDropOpen}
        onClose={() => !dropLoading && setConfirmDropOpen(false)}
        maxWidth="sm"
        fullWidth
      >
        <DialogTitle sx={{ color: "error.main", fontWeight: 700 }}>
          Drop All Neo4j Data?
        </DialogTitle>
        <DialogContent>
          <DialogContentText>
            This will permanently delete all nodes and relationships from Neo4j.
            This action cannot be undone.
          </DialogContentText>
        </DialogContent>
        <DialogActions>
          <Button
            onClick={() => setConfirmDropOpen(false)}
            disabled={dropLoading}
          >
            Cancel
          </Button>
          <Button
            color="error"
            variant="contained"
            onClick={handleDropAll}
            disabled={dropLoading}
          >
            {dropLoading ? "Dropping..." : "Yes, Drop All"}
          </Button>
        </DialogActions>
      </Dialog>
    </ThemeProvider>
  );
}
