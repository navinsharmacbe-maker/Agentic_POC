import React, { useEffect, useMemo, useRef, useState } from "react";
import {
  Box,
  Button,
  Chip,
  CircularProgress,
  Container,
  CssBaseline,
  Dialog,
  DialogActions,
  DialogContent,
  Fade,
  IconButton,
  Paper,
  Stack,
  TextField,
  ThemeProvider,
  Typography,
  createTheme
} from "@mui/material";
import {
  AutoAwesome,
  Brightness4,
  Brightness7,
  Check,
  ContentCopy,
  Send,
  Storage,
  UploadFile,
  Home
} from "@mui/icons-material";
import { Link } from "react-router-dom";
import LineageGraph from "../components/LineageGraph";

const SqlDisplay = ({ sql }) => {
  const [copied, setCopied] = useState(false);
  const lines = String(sql || "").split("\n");

  const handleCopy = () => {
    navigator.clipboard.writeText(sql || "");
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
        boxShadow: "0 16px 28px rgba(0,0,0,0.35)"
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
            SQL PREVIEW
          </Typography>
          <Chip size="small" label={`${lines.length} lines`} sx={{ height: 20, fontSize: 10 }} />
        </Stack>
        <Button
          size="small"
          startIcon={copied ? <Check /> : <ContentCopy />}
          onClick={handleCopy}
          sx={{ color: copied ? "#4ade80" : "#94a3b8", fontSize: 10 }}
        >
          {copied ? "COPIED" : "COPY SQL"}
        </Button>
      </Box>

      <Box
        sx={{
          bgcolor: "#0b1113",
          color: "#d4d4d4",
          fontFamily: "'Fira Code', monospace",
          fontSize: "0.82rem",
          whiteSpace: "pre",
          overflow: "auto",
          maxHeight: 420
        }}
      >
        {lines.map((line, idx) => (
          <Box key={idx} sx={{ display: "grid", gridTemplateColumns: "54px 1fr", minHeight: 24 }}>
            <Box
              sx={{
                px: 1.5,
                py: 0.4,
                textAlign: "right",
                color: "rgba(148,163,184,0.7)",
                borderRight: "1px solid rgba(148,163,184,0.16)",
                bgcolor: "rgba(15,23,42,0.42)",
                userSelect: "none"
              }}
            >
              {idx + 1}
            </Box>
            <Box sx={{ px: 1.8, py: 0.4 }}>{line || " "}</Box>
          </Box>
        ))}
      </Box>
    </Box>
  );
};

export default function SqlGenerationPage() {
  const [mode, setMode] = useState("dark");
  const [file, setFile] = useState(null);
  const [uploadStatus, setUploadStatus] = useState("");
  const [loading, setLoading] = useState(false);
  const [input, setInput] = useState("");
  const [generatedSql, setGeneratedSql] = useState("");
  const [pipelineNodes, setPipelineNodes] = useState([]);
  const [activeNode, setActiveNode] = useState("");

  const [uploadDialogOpen, setUploadDialogOpen] = useState(false);
  const [uploadProgress, setUploadProgress] = useState(0);
  const [visualUploadProgress, setVisualUploadProgress] = useState(0);
  const [uploadPhase, setUploadPhase] = useState("idle"); // idle | uploading | success | error
  const [uploadMessage, setUploadMessage] = useState("");

  const [sessionId, setSessionId] = useState(() => localStorage.getItem("sql_session_id") || "");
  const [messages, setMessages] = useState([
    { role: "assistant", content: "Ready. Upload a CSV or ask a question about the existing model." }
  ]);
  const scrollRef = useRef(null);
  const NODE_LABELS = {
    detect_intent: "Detect Intent",
    orchestrator: "Orchestrator",
    pruning: "Pruning",
    sql_generate: "SQL Generate",
    sql_modify: "SQL Modify",
    schema: "Schema",
    business: "Business",
    chat: "Chat"
  };

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [messages]);

  useEffect(() => {
    if (!uploadDialogOpen) return;
    const timer = setInterval(() => {
      setVisualUploadProgress((prev) => {
        const diff = uploadProgress - prev;
        if (Math.abs(diff) < 0.2) return uploadProgress;
        const step = Math.max(0.25, Math.min(2.2, Math.abs(diff) * 0.08));
        return prev + Math.sign(diff) * step;
      });
    }, 40);
    return () => clearInterval(timer);
  }, [uploadDialogOpen, uploadProgress]);

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

  const uploadWithProgress = (formData) =>
    new Promise((resolve, reject) => {
      const xhr = new XMLHttpRequest();
      xhr.open("POST", "http://localhost:8000/csv/upload");

      xhr.upload.onprogress = (event) => {
        if (!event.lengthComputable) return;
        const percent = Math.max(2, Math.min(92, Math.round((event.loaded / event.total) * 100)));
        setUploadProgress(percent);
      };

      xhr.onload = () => {
        if (xhr.status >= 200 && xhr.status < 300) {
          try {
            resolve(JSON.parse(xhr.responseText || "{}"));
          } catch {
            reject(new Error("Invalid response from upload API"));
          }
          return;
        }
        reject(new Error(`Upload failed (${xhr.status})`));
      };

      xhr.onerror = () => reject(new Error("Network error during upload"));
      xhr.send(formData);
    });

  const handleUpload = async () => {
    if (!file) return;
    try {
      setUploadStatus("Uploading...");
      setUploadDialogOpen(true);
      setUploadPhase("uploading");
      setUploadProgress(0);
      setVisualUploadProgress(0);
      setUploadMessage("Uploading metadata file...");

      const formData = new FormData();
      formData.append("files", file);
      const data = await uploadWithProgress(formData);

      if (data.session_id) {
        localStorage.setItem("sql_session_id", data.session_id);
        setSessionId(data.session_id);
      }

      setUploadProgress(100);
      setUploadPhase("success");
      setUploadMessage("Upload complete. Session is active.");
      setUploadStatus("Success!");
      setMessages((prev) => [
        ...prev,
        { role: "assistant", content: `Upload complete. Session ${data.session_id?.substring(0, 8)} is active.` }
      ]);
      setTimeout(() => setUploadDialogOpen(false), 1800);
    } catch (err) {
      console.error("Upload Error:", err);
      setUploadStatus("Failed");
      setUploadPhase("error");
      setUploadMessage("Upload failed. Please retry.");
    }
  };

  const handleSend = async () => {
    if (!input.trim() || loading) return;
    if (!sessionId) {
      setMessages((prev) => [...prev, { role: "assistant", content: "No active session. Please upload a file first." }]);
      return;
    }

    const userInput = input;
    setMessages((prev) => [...prev, { role: "user", content: userInput }]);
    setInput("");
    setLoading(true);
    setPipelineNodes([]);
    setActiveNode("");

    try {
      // Create placeholder assistant bubble for live streaming
      let assistantIndex = -1;
      setMessages((prev) => {
        assistantIndex = prev.length;
        return [...prev, { role: "assistant", content: "" }];
      });

      const response = await fetch("http://localhost:8000/chat/stream", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ session_id: sessionId, message: userInput })
      });

      if (!response.ok || !response.body) throw new Error("Agent communication failed");

      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      let buffer = "";
      let fullReply = "";

      const extractSqlProgress = (text) => {
        const start = text.search(/```sql/i);
        if (start === -1) return null;
        const afterStart = text.slice(start).replace(/```sql\s*/i, "");
        const end = afterStart.indexOf("```");
        const sql = end >= 0 ? afterStart.slice(0, end) : afterStart;
        const clean = sql.trim();
        return clean || null;
      };

      const updateAssistant = (text) => {
        setMessages((prev) =>
          prev.map((m, i) => (i === assistantIndex ? { ...m, content: text } : m))
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
            const progressiveSql = extractSqlProgress(fullReply);
            if (progressiveSql) setGeneratedSql(progressiveSql);
          } else if (eventName === "progress") {
            const node = String(payload.node || "").trim();
            if (node) {
              setActiveNode(node);
              setPipelineNodes((prev) => (prev.includes(node) ? prev : [...prev, node]));
            }
          } else if (eventName === "error") {
            const msg = payload.error || "System error: failed to communicate with agent.";
            updateAssistant(msg);
            setActiveNode("");
          } else if (eventName === "done") {
            const finalReply = payload.chat_reply || fullReply;
            if (finalReply) {
              fullReply = finalReply;
              updateAssistant(fullReply);
              const progressiveSql = extractSqlProgress(fullReply);
              if (progressiveSql) setGeneratedSql(progressiveSql);
            }
            setActiveNode("");
          }
        }
      }

      const sqlMatch = fullReply.match(/```sql\s*([\s\S]*?)```/i);
      const extractedSql = sqlMatch ? sqlMatch[1].trim() : null;
      if (extractedSql) setGeneratedSql(extractedSql);

      // keep chat bubble clean while SQL appears in preview pane
      setMessages((prev) =>
        prev.map((m, i) =>
          i === assistantIndex
            ? { ...m, content: (fullReply.replace(/```sql[\s\S]*?```/i, "").trim() || "SQL generated.") }
            : m
        )
      );
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
        <Container maxWidth="xl" sx={{ display: "flex", flexDirection: "column", py: 3 }}>
          <Box sx={{ display: "flex", alignItems: "center", justifyContent: "space-between", mb: 3, flexShrink: 0 }}>
            <Box sx={{ display: "flex", alignItems: "center", gap: 1.5 }}>
              <IconButton component={Link} to="/" color="primary" aria-label="Go to landing page">
                <Home />
              </IconButton>
              <Box>
                <Typography variant="h3" sx={{ fontWeight: 800, letterSpacing: -1 }}>SQL Generation Studio</Typography>
                <Typography sx={{ color: "text.secondary" }}>Production SQL authoring workspace</Typography>
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

          <Paper
            elevation={8}
            sx={{
              p: 2,
              mb: 3,
              display: "flex",
              alignItems: "center",
              justifyContent: "space-between",
              borderRadius: 4,
              flexShrink: 0
            }}
          >
            <Stack direction="row" spacing={2} alignItems="center">
              <Button component="label" variant="outlined" startIcon={<UploadFile />} sx={{ borderRadius: 10 }}>
                Choose File
                <input type="file" hidden onChange={(e) => { setFile(e.target.files?.[0]); setUploadStatus(""); }} />
              </Button>
              <Button
                variant="contained"
                startIcon={uploadStatus === "Uploading..." ? <CircularProgress size={20} color="inherit" /> : <AutoAwesome />}
                onClick={handleUpload}
                disabled={!file || uploadStatus === "Uploading..."}
                color={uploadStatus === "Success!" ? "success" : "primary"}
              >
                {uploadStatus === "Success!" ? "Uploaded" : "Upload to Server"}
              </Button>
              <Typography variant="body2" sx={{ color: "text.secondary" }}>
                {file ? file.name : sessionId ? "Session active" : "No file selected"}
              </Typography>
            </Stack>
            <Box sx={{ px: 2, py: 0.5, bgcolor: "rgba(30,215,96,0.1)", borderRadius: 5, color: "primary.main", fontWeight: 700, fontSize: 12 }}>
              SESSION: {sessionId ? sessionId.substring(0, 8) : "NONE"}
            </Box>
          </Paper>

          <Stack spacing={3} sx={{ mb: 2 }}>
            <Paper elevation={12} sx={{ display: "flex", flexDirection: "column", overflow: "hidden", bgcolor: "#000", position: "relative", height: "78vh", minHeight: 540 }}>
              <Box sx={{ p: 2, display: "flex", justifyContent: "space-between", alignItems: "center", borderBottom: "1px solid rgba(255,255,255,0.1)" }}>
                <Typography variant="h6" sx={{ fontWeight: 700, fontSize: 16 }}>Vis.js Lineage Graph</Typography>
                <Typography variant="caption" sx={{ color: "primary.main", fontWeight: 800 }}>LIVE VIEW</Typography>
              </Box>
              <Box sx={{ flex: 1, position: "relative", overflow: "hidden" }}>
                <Box sx={{ position: "absolute", inset: 0 }}>
                  <LineageGraph />
                </Box>
              </Box>
            </Paper>

            <Box sx={{ display: "grid", gridTemplateColumns: { xs: "1fr", md: "1fr 1fr" }, gap: 3 }}>
              <Paper
                elevation={12}
                sx={{
                  display: "flex",
                  flexDirection: "column",
                  border: "1px solid rgba(255,255,255,0.06)",
                  p: 2,
                  minHeight: 420,
                  bgcolor: "background.paper"
                }}
              >
                <Box sx={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                  <Typography variant="h6" sx={{ fontWeight: 700, fontSize: 16 }}>SQL Preview</Typography>
                  <Chip size="small" label={generatedSql ? "Updated" : "Awaiting output"} color={generatedSql ? "success" : "default"} />
                </Box>
                <Typography variant="caption" sx={{ color: "text.secondary", mt: 0.7 }}>
                  Generated SQL is pinned here for review and copy.
                </Typography>
                {generatedSql ? (
                  <SqlDisplay sql={generatedSql} />
                ) : (
                  <Box sx={{ mt: 2, p: 2, borderRadius: 2, border: "1px dashed rgba(148,163,184,0.35)", bgcolor: "rgba(2,6,23,0.25)" }}>
                    <Typography variant="body2" sx={{ color: "text.secondary" }}>
                      Run a prompt in chat. The latest generated SQL will appear here in editor view.
                    </Typography>
                  </Box>
                )}
              </Paper>

	              <Paper elevation={12} sx={{ display: "flex", flexDirection: "column", overflow: "hidden", border: "1px solid rgba(255,255,255,0.05)", minHeight: 420, maxHeight: 700 }}>
	                <Box sx={{ p: 2, borderBottom: "1px solid rgba(255,255,255,0.05)", display: "flex", justifyContent: "space-between", gap: 1.5 }}>
	                  <Typography variant="h6" sx={{ fontWeight: 700, fontSize: 16 }}>Chat for SQL</Typography>
	                  <Storage sx={{ color: "primary.main", fontSize: 18, mt: 0.2 }} />
	                </Box>
	                <Box sx={{ px: 2, py: 1.2, borderBottom: "1px solid rgba(255,255,255,0.05)", display: "flex", flexWrap: "wrap", gap: 0.8 }}>
	                  {pipelineNodes.length === 0 ? (
	                    <Chip size="small" label={loading ? "Waiting for pipeline..." : "No active pipeline"} variant="outlined" />
	                  ) : (
	                    pipelineNodes.map((node) => (
	                      <Chip
	                        key={node}
	                        size="small"
	                        label={NODE_LABELS[node] || node}
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
                          <Typography variant="body2" sx={{ lineHeight: 1.6 }}>{msg.content}</Typography>
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
                      placeholder="Ask for SQL..."
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
          </Stack>
        </Container>
      </Box>

      <Dialog
        open={uploadDialogOpen}
        onClose={uploadPhase === "uploading" ? undefined : () => setUploadDialogOpen(false)}
        maxWidth="xs"
        fullWidth
      >
        <DialogContent
          sx={{
            pt: 3,
            pb: 2,
            "@keyframes rainbowFlow": {
              "0%": { backgroundPosition: "0% 50%" },
              "100%": { backgroundPosition: "180% 50%" }
            },
            "@keyframes liquidShine": {
              "0%": { transform: "translateX(-140%)" },
              "100%": { transform: "translateX(240%)" }
            }
          }}
        >
          <Typography sx={{ fontWeight: 700, mb: 1.2 }}>
            {uploadPhase === "success" ? "Upload Successful" : uploadPhase === "error" ? "Upload Failed" : "Uploading Document"}
          </Typography>
          <Typography variant="body2" sx={{ color: "text.secondary", mb: 2 }}>
            {uploadMessage || "Preparing upload..."}
          </Typography>

          <Box
            sx={{
              height: 18,
              borderRadius: 999,
              bgcolor: "rgba(148,163,184,0.2)",
              overflow: "hidden",
              border: "1px solid rgba(148,163,184,0.28)"
            }}
          >
            <Box
              sx={{
                width: `${visualUploadProgress}%`,
                height: "100%",
                transition: "width 340ms ease-out",
                borderRadius: "inherit",
                position: "relative",
                overflow: "hidden",
                background:
                  uploadPhase === "error"
                    ? "linear-gradient(90deg, #ef4444, #dc2626)"
                    : "linear-gradient(110deg, #16a34a, #22c55e, #10b981, #14b8a6, #0ea5e9, #3b82f6)",
                backgroundSize: "200% 100%",
                animation: uploadPhase === "error" ? "none" : "rainbowFlow 4.8s linear infinite",
                "&::after": {
                  content: '""',
                  position: "absolute",
                  inset: 0,
                  background: "linear-gradient(110deg, transparent 20%, rgba(255,255,255,0.18) 48%, transparent 75%)",
                  animation: uploadPhase === "error" ? "none" : "liquidShine 3.6s ease-in-out infinite"
                }
              }}
            />
          </Box>

          <Typography variant="caption" sx={{ display: "block", mt: 1, color: "text.secondary" }}>
            {Math.round(visualUploadProgress)}% completed
          </Typography>
        </DialogContent>
        {uploadPhase !== "uploading" && (
          <DialogActions sx={{ px: 3, pb: 2 }}>
            <Button onClick={() => setUploadDialogOpen(false)} variant="contained">
              {uploadPhase === "success" ? "Done" : "Close"}
            </Button>
          </DialogActions>
        )}
      </Dialog>
    </ThemeProvider>
  );
}
