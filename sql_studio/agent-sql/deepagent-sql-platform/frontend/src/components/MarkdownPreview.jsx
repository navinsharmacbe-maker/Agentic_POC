import React, { useEffect, useState } from "react";
import ReactMarkdown from "react-markdown";
import { fetchMarkdown } from "../services/api";

export default function MarkdownPreview({ sessionId }) {
  const [content, setContent] = useState("# Documentation will appear here...");

  useEffect(() => {
    if (!sessionId) return;

    const fetchContent = async () => {
      try {
        const res = await fetchMarkdown(sessionId);
        if (res?.data?.markdown) {
          setContent(res.data.markdown);
        }
      } catch (err) {
        console.error("Failed to fetch markdown:", err);
      }
    };

    // Initial fetch
    fetchContent();

    // Polling every 4s
    const interval = setInterval(fetchContent, 4000);

    return () => clearInterval(interval);
  }, [sessionId]);

  return (
    <div style={{ overflowY: "auto", height: "100%", padding: "1rem" }}>
      <ReactMarkdown>{content}</ReactMarkdown>
    </div>
  );
}
