import { Box, Divider, Paper, Typography } from "@mui/material";
import LayersIcon from "@mui/icons-material/Layers";
import StorageIcon from "@mui/icons-material/Storage";
import AccountTreeIcon from "@mui/icons-material/AccountTree";
import TableChartIcon from "@mui/icons-material/TableChart";

const iconByTitle = {
  Summary: <LayersIcon sx={{ fontSize: 20, color: "primary.main" }} />,
  ODP: <StorageIcon sx={{ fontSize: 20, color: "primary.main" }} />,
  FDP: <AccountTreeIcon sx={{ fontSize: 20, color: "primary.main" }} />,
  CDP: <TableChartIcon sx={{ fontSize: 20, color: "primary.main" }} />
};

export default function MetricCard({ title, metrics, index = 0 }) {
  return (
    <Paper
      elevation={0}
      sx={{
        p: 2.5,
        borderRadius: 2,
        bgcolor: "background.paper",
        border: "1px solid",
        borderColor: "divider",
        boxShadow: "0 10px 24px rgba(0,0,0,0.18)",
        transition: "transform 300ms ease, box-shadow 300ms ease",
        animation: "metricFadeIn 450ms ease both",
        animationDelay: `${index * 100}ms`,
        "&:hover": {
          transform: "scale(1.02)",
          boxShadow: "0 12px 30px rgba(30,215,96,0.24)"
        },
        "@keyframes metricFadeIn": {
          from: { opacity: 0, transform: "translateY(8px)" },
          to: { opacity: 1, transform: "translateY(0)" }
        }
      }}
    >
      <Box sx={{ display: "flex", alignItems: "center", gap: 1.25, mb: 1.5 }}>
        {iconByTitle[title] || iconByTitle.Summary}
        <Typography sx={{ fontWeight: 700, letterSpacing: 0.4, fontSize: 13, textTransform: "uppercase" }}>
          {title}
        </Typography>
      </Box>
      <Divider sx={{ mb: 1.5, opacity: 0.7 }} />
      <Box sx={{ display: "grid", gap: 1 }}>
        {metrics.map((m) => (
          <Box key={m.label} sx={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
            <Typography sx={{ color: "text.secondary", fontSize: 14 }}>{m.label}</Typography>
            <Typography sx={{ fontFamily: "monospace", fontWeight: 700, fontSize: 14 }}>{m.value}</Typography>
          </Box>
        ))}
      </Box>
    </Paper>
  );
}
