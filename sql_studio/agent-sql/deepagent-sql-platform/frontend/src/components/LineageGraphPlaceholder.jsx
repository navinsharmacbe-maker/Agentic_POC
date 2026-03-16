import { useEffect, useRef } from "react";
import { fetchCanvasLineageGraph } from "../services/api";

export default function LineageGraphPlaceholder({ refresh }) {
  const canvasRef = useRef(null);
  const graphRef = useRef(null);
  const viewRef = useRef({ scale: 1, offsetX: 0, offsetY: 0 });
  const dragRef = useRef({ active: false, startX: 0, startY: 0, startOffsetX: 0, startOffsetY: 0 });
  const layoutRef = useRef({ nodes: [], edges: [], layers: [] });
  const drawRef = useRef(() => {});
  const hasAutoFitRef = useRef(false);

  useEffect(() => {
    let isActive = true;

    fetchCanvasLineageGraph()
      .then((data) => {
        if (!isActive) return;
        graphRef.current = data;
        hasAutoFitRef.current = false;
        window.dispatchEvent(new Event("canvas-lineage-data-updated"));
      })
      .catch((err) => {
        if (!isActive) return;
        console.error("Canvas lineage API error:", err);
        graphRef.current = null;
        hasAutoFitRef.current = false;
        window.dispatchEvent(new Event("canvas-lineage-data-updated"));
      });

    return () => {
      isActive = false;
    };
  }, [refresh]);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;

    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    const fitToContent = () => {
      const rect = canvas.getBoundingClientRect();
      const nodes = layoutRef.current.nodes;
      if (!nodes.length || !rect.width || !rect.height) return;

      const nodeW = 180;
      const nodeH = 44;
      let minX = Infinity;
      let minY = Infinity;
      let maxX = -Infinity;
      let maxY = -Infinity;

      nodes.forEach((n) => {
        minX = Math.min(minX, n.x - nodeW / 2);
        minY = Math.min(minY, n.y - nodeH / 2);
        maxX = Math.max(maxX, n.x + nodeW / 2);
        maxY = Math.max(maxY, n.y + nodeH / 2);
      });

      const pad = 110;
      const worldW = Math.max(1, maxX - minX + pad * 2);
      const worldH = Math.max(1, maxY - minY + pad * 2);
      const scale = Math.max(0.28, Math.min(1.6, Math.min(rect.width / worldW, rect.height / worldH)));

      viewRef.current = {
        scale,
        offsetX: rect.width / 2 - ((minX + maxX) / 2) * scale,
        offsetY: rect.height / 2 - ((minY + maxY) / 2) * scale
      };
    };

    const draw = () => {
      const dpr = window.devicePixelRatio || 1;
      const rect = canvas.getBoundingClientRect();

      canvas.width = Math.max(1, Math.floor(rect.width * dpr));
      canvas.height = Math.max(1, Math.floor(rect.height * dpr));

      ctx.setTransform(1, 0, 0, 1, 0, 0);
      ctx.scale(dpr, dpr);

      const w = rect.width;
      const h = rect.height;

      ctx.clearRect(0, 0, w, h);
      const graphData = graphRef.current;
      const { scale, offsetX, offsetY } = viewRef.current;
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

      const layers = graphData?.layers?.length ? graphData.layers : ["ODP", "FDP", "CDP"];
      const sourceNodes = graphData?.nodes?.length
        ? graphData.nodes
        : layers.flatMap((layer, li) =>
            Array.from({ length: 3 + ((li + 1) % 2) }).map((_, i) => ({
              id: `${layer}_T${i + 1}`,
              label: `${layer}_T${i + 1}`,
              layer
            }))
          );

      const grouped = layers.reduce((acc, layer) => {
        acc[layer] = sourceNodes.filter((n) => n.layer === layer);
        return acc;
      }, {});

      const nodes = [];
      layers.forEach((layer, li) => {
        const layerNodes = grouped[layer] || [];
        const laneX = li * 380;
        const perCol = 12;
        layerNodes.forEach((node, i) => {
          const col = Math.floor(i / perCol);
          const row = i % perCol;
          const x = typeof node.x === "number" ? node.x : laneX + col * 210;
          const y = typeof node.y === "number" ? node.y : 90 + row * 72;
          nodes.push({
            id: node.id || `${layer}_${i + 1}`,
            x,
            y,
            label: node.label || node.id || `${layer}_${i + 1}`,
            layerIndex: li,
            layer
          });
        });
      });
      const nodeMap = new Map(nodes.map((n) => [n.id, n]));
      const edges = graphData?.edges?.length
        ? graphData.edges
        : nodes
            .flatMap((from) =>
              nodes
                .filter((to) => to.layerIndex === from.layerIndex + 1)
                .filter((_, i) => i % 2 === 0)
                .map((to) => ({ from: from.id, to: to.id }))
            );

      layoutRef.current = { nodes, edges, layers };
      if (!hasAutoFitRef.current && nodes.length) {
        fitToContent();
        hasAutoFitRef.current = true;
        drawRef.current();
        return;
      }

      ctx.lineWidth = 1.5;
      edges.forEach((edge) => {
        const node = nodeMap.get(edge.from);
        const target = nodeMap.get(edge.to);
        if (!node || !target) return;
        const gradient = ctx.createLinearGradient(node.x, node.y, target.x, target.y);
        gradient.addColorStop(0, "rgba(30,215,96,0.3)");
        gradient.addColorStop(1, "rgba(11,95,255,0.3)");
        ctx.strokeStyle = gradient;
        ctx.beginPath();
        ctx.moveTo(node.x, node.y);
        const cpx = (node.x + target.x) / 2;
        ctx.bezierCurveTo(cpx, node.y, cpx, target.y, target.x, target.y);
        ctx.stroke();
      });

      const colors = ["rgba(30,215,96,0.9)", "rgba(11,95,255,0.9)", "rgba(239,68,68,0.88)"];
      const bgColors = ["rgba(30,215,96,0.12)", "rgba(11,95,255,0.12)", "rgba(239,68,68,0.12)"];
      const nodeW = 180;
      const nodeH = 44;

      nodes.forEach((node) => {
        const roundRect = (x, y, width, height, radius) => {
          ctx.beginPath();
          if (typeof ctx.roundRect === "function") {
            ctx.roundRect(x, y, width, height, radius);
          } else {
            const r = Math.min(radius, width / 2, height / 2);
            ctx.moveTo(x + r, y);
            ctx.arcTo(x + width, y, x + width, y + height, r);
            ctx.arcTo(x + width, y + height, x, y + height, r);
            ctx.arcTo(x, y + height, x, y, r);
            ctx.arcTo(x, y, x + width, y, r);
            ctx.closePath();
          }
        };

        ctx.shadowColor = colors[node.layerIndex] || colors[0];
        ctx.shadowBlur = 14;
        ctx.fillStyle = bgColors[node.layerIndex] || bgColors[0];
        roundRect(node.x - nodeW / 2, node.y - nodeH / 2, nodeW, nodeH, 10);
        ctx.fill();

        ctx.shadowBlur = 0;
        ctx.strokeStyle = colors[node.layerIndex] || colors[0];
        ctx.lineWidth = 1;
        roundRect(node.x - nodeW / 2, node.y - nodeH / 2, nodeW, nodeH, 10);
        ctx.stroke();

        ctx.fillStyle = "rgba(255,255,255,0.9)";
        ctx.font = "11px 'JetBrains Mono', monospace";
        ctx.textAlign = "center";
        ctx.textBaseline = "middle";
        const label = String(node.label).length > 24 ? `${String(node.label).slice(0, 24)}...` : String(node.label);
        ctx.fillText(label, node.x, node.y);
      });

      layers.forEach((layer, li) => {
        const cx = li * 380;
        ctx.fillStyle = "rgba(255,255,255,0.35)";
        ctx.font = "700 13px 'Poppins', sans-serif";
        ctx.textAlign = "center";
        ctx.fillText(`${layer} Layer`, cx, 24);
      });

      ctx.restore();
    };

    drawRef.current = draw;
    draw();

    const zoomBy = (factor) => {
      const rect = canvas.getBoundingClientRect();
      const mx = rect.width / 2;
      const my = rect.height / 2;
      const { scale, offsetX, offsetY } = viewRef.current;
      const nextScale = Math.max(0.28, Math.min(2.8, scale * factor));
      const wx = (mx - offsetX) / scale;
      const wy = (my - offsetY) / scale;
      viewRef.current = {
        scale: nextScale,
        offsetX: mx - wx * nextScale,
        offsetY: my - wy * nextScale
      };
      draw();
    };

    const fitGraph = () => {
      if (!layoutRef.current.nodes.length) return;
      const rect = canvas.getBoundingClientRect();
      const nodeW = 180;
      const nodeH = 44;
      let minX = Infinity;
      let minY = Infinity;
      let maxX = -Infinity;
      let maxY = -Infinity;

      layoutRef.current.nodes.forEach((n) => {
        minX = Math.min(minX, n.x - nodeW / 2);
        minY = Math.min(minY, n.y - nodeH / 2);
        maxX = Math.max(maxX, n.x + nodeW / 2);
        maxY = Math.max(maxY, n.y + nodeH / 2);
      });

      const pad = 110;
      const worldW = Math.max(1, maxX - minX + pad * 2);
      const worldH = Math.max(1, maxY - minY + pad * 2);
      const scale = Math.max(0.28, Math.min(1.6, Math.min(rect.width / worldW, rect.height / worldH)));
      viewRef.current = {
        scale,
        offsetX: rect.width / 2 - ((minX + maxX) / 2) * scale,
        offsetY: rect.height / 2 - ((minY + maxY) / 2) * scale
      };
      draw();
    };

    const onWheel = (e) => {
      e.preventDefault();
      const rect = canvas.getBoundingClientRect();
      const mx = e.clientX - rect.left;
      const my = e.clientY - rect.top;
      const { scale, offsetX, offsetY } = viewRef.current;
      const nextScale = Math.max(0.28, Math.min(2.8, scale * (e.deltaY < 0 ? 1.12 : 0.9)));
      const wx = (mx - offsetX) / scale;
      const wy = (my - offsetY) / scale;
      viewRef.current = {
        scale: nextScale,
        offsetX: mx - wx * nextScale,
        offsetY: my - wy * nextScale
      };
      draw();
    };

    const onZoomInCmd = () => zoomBy(1.12);
    const onZoomOutCmd = () => zoomBy(0.9);
    const onFitCmd = () => fitGraph();

    const onMouseDown = (e) => {
      const rect = canvas.getBoundingClientRect();
      dragRef.current = {
        active: true,
        startX: e.clientX - rect.left,
        startY: e.clientY - rect.top,
        startOffsetX: viewRef.current.offsetX,
        startOffsetY: viewRef.current.offsetY
      };
      canvas.style.cursor = "grabbing";
    };

    const onMouseMove = (e) => {
      if (!dragRef.current.active) return;
      const rect = canvas.getBoundingClientRect();
      const mx = e.clientX - rect.left;
      const my = e.clientY - rect.top;
      viewRef.current.offsetX = dragRef.current.startOffsetX + (mx - dragRef.current.startX);
      viewRef.current.offsetY = dragRef.current.startOffsetY + (my - dragRef.current.startY);
      draw();
    };

    const onMouseUp = () => {
      dragRef.current.active = false;
      canvas.style.cursor = "grab";
    };

    canvas.style.cursor = "grab";
    canvas.addEventListener("wheel", onWheel, { passive: false });
    canvas.addEventListener("mousedown", onMouseDown);
    window.addEventListener("mousemove", onMouseMove);
    window.addEventListener("mouseup", onMouseUp);
    window.addEventListener("resize", draw);
    window.addEventListener("canvas-lineage-data-updated", draw);
    window.addEventListener("canvas-lineage-zoom-in", onZoomInCmd);
    window.addEventListener("canvas-lineage-zoom-out", onZoomOutCmd);
    window.addEventListener("canvas-lineage-fit", onFitCmd);

    return () => {
      canvas.removeEventListener("wheel", onWheel);
      canvas.removeEventListener("mousedown", onMouseDown);
      window.removeEventListener("mousemove", onMouseMove);
      window.removeEventListener("mouseup", onMouseUp);
      window.removeEventListener("resize", draw);
      window.removeEventListener("canvas-lineage-data-updated", draw);
      window.removeEventListener("canvas-lineage-zoom-in", onZoomInCmd);
      window.removeEventListener("canvas-lineage-zoom-out", onZoomOutCmd);
      window.removeEventListener("canvas-lineage-fit", onFitCmd);
    };
  }, [refresh]);

  return (
    <div style={{ position: "relative", width: "100%", height: "100%" }}>
      <canvas
        ref={canvasRef}
        className="w-full h-full rounded-md"
        style={{ display: "block", width: "100%", height: "100%" }}
      />
    </div>
  );
}
