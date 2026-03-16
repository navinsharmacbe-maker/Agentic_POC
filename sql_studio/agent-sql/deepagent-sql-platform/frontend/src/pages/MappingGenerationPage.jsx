import React, { useEffect, useMemo, useRef, useState } from "react";
import {
  Box,
  Button,
  Chip,
  CircularProgress,
  Container,
  CssBaseline,
  Fade,
  IconButton,
  Paper,
  Stack,
  TextField,
  ThemeProvider,
  Typography,
  createTheme,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow
} from "@mui/material";
import {
  Brightness4,
  Brightness7,
  Check,
  ContentCopy,
  Send,
  Storage,
  Home
} from "@mui/icons-material";
import { Link } from "react-router-dom";

const MappingDisplay = ({ text }) => {
  const [copied, setCopied] = useState(false);
  
  let parsedData = null;
  let rawText = text || "";
  if (text) {
    const jsonMatch = text.match(/```json\s*([\s\S]*?)```/i);
    if (jsonMatch) {
      try {
        parsedData = JSON.parse(jsonMatch[1]);
      } catch (e) {
        console.error("Failed to parse mapping JSON", e);
      }
    }
  }

  const handleCopy = () => {
    navigator.clipboard.writeText(text || "");
    setCopied(true);
    setTimeout(() => setCopied(false), 1800);
  };

  return (
    <Box
      sx={{
        mt: 1.5,
        borderRadius: 2.5,
        overflow: "hidden",
        border: "1px solid rgba(255,255,255,0.14)",
        boxShadow: "0 16px 28px rgba(0,0,0,0.35)",
        display: "flex",
        flexDirection: "column",
        height: "100%"
      }}
    >
      <Box
        sx={{
          bgcolor: "rgba(18,24,23,0.96)",
          px: 2,
          py: 1.1,
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center"
        }}
      >
        <Stack direction="row" spacing={1} alignItems="center">
          <Typography variant="caption" sx={{ color: "#4ade80", fontWeight: 700, letterSpacing: 1 }}>
            MAPPING SHEET PREVIEW
          </Typography>
          {parsedData && <Chip size="small" label={`${parsedData.length} columns`} sx={{ height: 20, fontSize: 10, bgcolor: "rgba(255,255,255,0.1)" }} />}
        </Stack>
        <Button
          size="small"
          startIcon={copied ? <Check /> : <ContentCopy />}
          onClick={handleCopy}
          sx={{ color: copied ? "#4ade80" : "#94a3b8", fontSize: 10 }}
        >
          {copied ? "COPIED" : "COPY DATA"}
        </Button>
      </Box>

      <Box sx={{ flex: 1, overflow: "auto", bgcolor: "#0b1113", p: 0 }}>
        {parsedData && Array.isArray(parsedData) ? (
          <TableContainer>
            <Table size="small" sx={{ minWidth: 600 }}>
              <TableHead>
                <TableRow sx={{ bgcolor: "rgba(255,255,255,0.05)" }}>
                  <TableCell sx={{ color: "#4ade80", fontWeight: "bold" }}>Target Column</TableCell>
                  <TableCell sx={{ color: "#94a3b8" }}>Datatype</TableCell>
                  <TableCell sx={{ color: "#94a3b8" }}>Source Tables</TableCell>
                  <TableCell sx={{ color: "#94a3b8" }}>Source Columns</TableCell>
                  <TableCell sx={{ color: "#94a3b8" }}>Transform Logic</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {parsedData.map((row, idx) => (
                  <TableRow key={idx} sx={{ "&:last-child td, &:last-child th": { border: 0 } }}>
                    <TableCell sx={{ color: "#fff", fontWeight: 500 }}>{row.target_column}</TableCell>
                    <TableCell sx={{ color: "#cbd5e1" }}>{row.target_datatype}</TableCell>
                    <TableCell sx={{ color: "#cbd5e1" }}>{row.source_tables}</TableCell>
                    <TableCell sx={{ color: "#cbd5e1" }}>{row.source_columns}</TableCell>
                    <TableCell sx={{ color: "#cbd5e1", fontFamily: "monospace", fontSize: "0.8rem" }}>{row.transformation_logic}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
        ) : (
          <Box sx={{ p: 2, color: "#d4d4d4", fontFamily: "monospace", fontSize: "0.85rem", whiteSpace: "pre-wrap" }}>
            {rawText || " "}
          </Box>
        )}
      </Box>
    </Box>
  );
};

export default function MappingGenerationPage() {
  const [mode, setMode] = useState("dark");
  const [loading, setLoading] = useState(false);
  const [input, setInput] = useState("");
  const [generatedMapping, setGeneratedMapping] = useState("");
  const [pipelineNodes, setPipelineNodes] = useState([]);
  const [activeNode, setActiveNode] = useState("");

  const [sessionId] = useState(() => localStorage.getItem("sql_session_id") || "local_session_" + Date.now());
  const [messages, setMessages] = useState([
    { role: "assistant", content: "Ready to build a mapping sheet. Type what table you want to create (e.g. 'Create a CDP summary table for customer addresses')." }
  ]);
  const scrollRef = useRef(null);

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [messages]);

  const theme = useMemo(
    () =>
      createTheme({
        palette: {
          mode,
          primary: { main: mode === "dark" ? "#1ed760" : "#0b5fff" },
          background: {
            default: mode === "dark" ? "#0b0f0e" : "#f6f9ff",
            paper: mode === "dark" ? "#111716" : "#ffffff"
          }
        },
        shape: { borderRadius: 14 },
        typography: { fontFamily: '"Poppins","Montserrat",sans-serif' }
      }),
    [mode]
  );

  const handleSend = async () => {
    if (!input.trim() || loading) return;

    const userInput = input;
    setMessages((prev) => [...prev, { role: "user", content: userInput }]);
    setInput("");
    setLoading(true);
    setPipelineNodes([]);
    setActiveNode("");

    try {
      let assistantIndex = -1;
      setMessages((prev) => {
        assistantIndex = prev.length;
        return [...prev, { role: "assistant", content: "" }];
      });

      const response = await fetch("http://localhost:8000/mapping/chat/stream", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ session_id: sessionId, message: userInput })
      });

      if (!response.ok || !response.body) throw new Error("Agent communication failed");

      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      let buffer = "";
      let fullReply = "";

      const updateAssistant = (text) => {
        // Only show text outside the json block in chat if possible
        const chatText = text.replace(/```json[\s\S]*?```/i, "[Mapping Sheet Generated]").trim();
        setMessages((prev) =>
          prev.map((m, i) => (i === assistantIndex ? { ...m, content: chatText || "Processing..." } : m))
        );
      };

      while (true) {
        const { value, done } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });

        const events = buffer.split("\n\n");
        buffer = events.pop() || "";

        for (const evt of events) {
          const lines = evt.split("\n");
          let eventName = "message";
          let dataLine = "";
          for (const line of lines) {
            if (line.startsWith("event:")) eventName = line.replace("event:", "").trim();
            if (line.startsWith("data:")) dataLine += line.replace("data:", "").trim();
          }
          if (!dataLine) continue;

          let payload = {};
          try {
            payload = JSON.parse(dataLine);
          } catch {
            continue;
          }

          if (eventName === "chunk") {
            fullReply += payload.content || "";
            updateAssistant(fullReply);
            setGeneratedMapping(fullReply);
          } else if (eventName === "progress") {
            const node = String(payload.node || "").trim();
            if (node) {
              setActiveNode(node);
              setPipelineNodes((prev) => (prev.includes(node) ? prev : [...prev, node]));
            }
          } else if (eventName === "error") {
             setMessages((prev) =>
              prev.map((m, i) => (i === assistantIndex ? { ...m, content: "Error: " + payload.error } : m))
            );
            setActiveNode("");
          } else if (eventName === "done") {
            const finalReply = payload.chat_reply || fullReply;
            if (finalReply) {
              fullReply = finalReply;
              updateAssistant(fullReply);
              setGeneratedMapping(fullReply);
            }
            setActiveNode("");
          }
        }
      }
    } catch (err) {
      console.error("Fetch Error:", err);
      setMessages((prev) => [...prev, { role: "assistant", content: "System error: failed to communicate with agent." }]);
    } finally {
      setLoading(false);
    }
  };

  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <Box
        sx={{
          minHeight: "100vh",
          width: "100vw",
          overflowX: "hidden",
          overflowY: "auto",
          display: "flex",
          flexDirection: "column",
          background:
            mode === "dark"
              ? "radial-gradient(1100px 600px at 10% -10%, rgba(30,215,96,0.15), transparent 60%), #0b0f0e"
              : "radial-gradient(900px 500px at 15% -10%, rgba(11,95,255,0.1), transparent 60%), #f6f9ff"
        }}
      >
        <Container maxWidth="xl" sx={{ display: "flex", flexDirection: "column", py: 3, height: "100vh" }}>
          <Box sx={{ display: "flex", alignItems: "center", justifyContent: "space-between", mb: 3, flexShrink: 0 }}>
            <Box sx={{ display: "flex", alignItems: "center", gap: 1.5 }}>
              <IconButton component={Link} to="/" color="primary" aria-label="Go to landing page">
                <Home />
              </IconButton>
              <Box>
                <Typography variant="h3" sx={{ fontWeight: 800, letterSpacing: -1 }}>Mapping Builder</Typography>
                <Typography sx={{ color: "text.secondary" }}>Design transformations using the Neo4j schema</Typography>
              </Box>
            </Box>
            <Stack direction="row" spacing={2} alignItems="center">
              <Button component={Link} to="/app" variant="outlined" color="inherit" sx={{ borderRadius: 10 }}>
                Back to Main
              </Button>
              <IconButton onClick={() => setMode((m) => (m === "dark" ? "light" : "dark"))} color="primary">
                {mode === "dark" ? <Brightness7 /> : <Brightness4 />}
              </IconButton>
            </Stack>
          </Box>

          <Box sx={{ display: "flex", gap: 3, flex: 1, minHeight: 0 }}>
            <Paper
              elevation={12}
              sx={{
                flex: 2,
                display: "flex",
                flexDirection: "column",
                border: "1px solid rgba(255,255,255,0.06)",
                p: 2,
                bgcolor: "background.paper",
                overflow: "hidden"
              }}
            >
              <Box sx={{ display: "flex", justifyContent: "space-between", alignItems: "center", mb: 1 }}>
                <Typography variant="h6" sx={{ fontWeight: 700, fontSize: 16 }}>Mapping Sheet</Typography>
                <Chip size="small" label={generatedMapping ? "Populated" : "Awaiting agent"} color={generatedMapping ? "success" : "default"} />
              </Box>
              
              <Box sx={{ flex: 1, minHeight: 0, display: "flex", flexDirection: "column" }}>
                {generatedMapping ? (
                  <MappingDisplay text={generatedMapping} />
                ) : (
                  <Box sx={{ flex: 1, display: "flex", alignItems: "center", justifyContent: "center", mt: 2, p: 2, borderRadius: 2, border: "1px dashed rgba(148,163,184,0.35)", bgcolor: "rgba(2,6,23,0.25)" }}>
                    <Typography variant="body2" sx={{ color: "text.secondary", textAlign: "center" }}>
                      Chat with the agent to design a new layer table.<br/>
                      The intelligent agent will fetch the preceding layer from Neo4j<br/>
                      and generate the mapping sheet here.
                    </Typography>
                  </Box>
                )}
              </Box>
            </Paper>

            <Paper elevation={12} sx={{ flex: 1.2, display: "flex", flexDirection: "column", overflow: "hidden", border: "1px solid rgba(255,255,255,0.05)" }}>
              <Box sx={{ p: 2, borderBottom: "1px solid rgba(255,255,255,0.05)", display: "flex", justifyContent: "space-between", gap: 1.5 }}>
                <Typography variant="h6" sx={{ fontWeight: 700, fontSize: 16 }}>AI Assistant</Typography>
                <Storage sx={{ color: "primary.main", fontSize: 18, mt: 0.2 }} />
              </Box>
              <Box sx={{ px: 2, py: 1.2, borderBottom: "1px solid rgba(255,255,255,0.05)", display: "flex", flexWrap: "wrap", gap: 0.8 }}>
                {pipelineNodes.length === 0 ? (
                  <Chip size="small" label={loading ? "Running agent tasks..." : "Ready"} variant="outlined" />
                ) : (
                  pipelineNodes.map((node) => (
                    <Chip
                      key={node}
                      size="small"
                      label={node}
                      color={activeNode === node ? "primary" : "success"}
                      variant={activeNode === node ? "filled" : "outlined"}
                    />
                  ))
                )}
              </Box>

              <Box ref={scrollRef} sx={{ flex: 1, overflowY: "auto", p: 2.2, display: "flex", flexDirection: "column", gap: 2.2 }}>
                {messages.map((msg, idx) => (
                  <Fade in={true} key={idx}>
                    <Box sx={{ alignSelf: msg.role === "user" ? "flex-end" : "flex-start", maxWidth: "94%" }}>
                      <Box
                        sx={{
                          p: 1.8,
                          borderRadius: 2.3,
                          bgcolor: msg.role === "user" ? "primary.main" : "background.default",
                          color: msg.role === "user" ? "#fff" : "text.primary",
                          boxShadow: 3,
                          border: msg.role === "assistant" ? "1px solid rgba(255,255,255,0.06)" : "none"
                        }}
                      >
                        <Typography variant="body2" sx={{ lineHeight: 1.6, whiteSpace: "pre-wrap" }}>{msg.content}</Typography>
                      </Box>
                    </Box>
                  </Fade>
                ))}
                {loading && <CircularProgress size={24} sx={{ m: 2, alignSelf: "center" }} />}
              </Box>

              <Box sx={{ p: 2, bgcolor: "background.paper" }}>
                <Box sx={{ display: "flex", gap: 1, alignItems: "center", bgcolor: "background.default", p: 1, borderRadius: 10, border: "1px solid rgba(255,255,255,0.1)" }}>
                  <TextField
                    fullWidth
                    variant="standard"
                    placeholder="e.g. Create FDP layer for new patient records..."
                    value={input}
                    onChange={(e) => setInput(e.target.value)}
                    onKeyPress={(e) => e.key === "Enter" && handleSend()}
                    InputProps={{ disableUnderline: true, sx: { px: 2 } }}
                  />
                  <IconButton color="primary" onClick={handleSend} disabled={loading} sx={{ bgcolor: "primary.main", color: "#000", "&:hover": { bgcolor: "primary.dark" } }}>
                    <Send fontSize="small" />
                  </IconButton>
                </Box>
              </Box>
            </Paper>
          </Box>
        </Container>
      </Box>
    </ThemeProvider>
  );
}
