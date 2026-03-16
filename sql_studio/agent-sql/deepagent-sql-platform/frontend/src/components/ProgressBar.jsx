import React from "react";

export default function ProgressBar({ value = 0 }) {
  return (
    <div style={{ border: "1px solid #ccc", width: "300px", borderRadius: "5px" }}>
      <div
        style={{
          width: `${value}%`,
          background: "green",
          height: "20px",
          borderRadius: "5px",
        }}
      ></div>
    </div>
  );
}
