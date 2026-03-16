import React, { useState } from "react";
import { startProcessing } from "../services/api";

export default function RepoPathForm() {
  const [path, setPath] = useState("");

  const handleStart = async () => {
    if (!path) return alert("Enter repo path");
    try {
      await startProcessing({ path });
      alert("Processing started for " + path);
    } catch (err) {
      console.error(err);
      let msg = "Error starting processing.";
      if (err.code === "ERR_NETWORK") {
        msg += " Is the backend server running at http://localhost:8000?";
      } else if (err.response && err.response.data && err.response.data.detail) {
        msg += " " + err.response.data.detail;
      } else {
        msg += " " + err.message;
      }
      alert(msg);
    }
  };

  return (
    <div>
      <input
        type="text"
        placeholder="Enter local repo path"
        value={path}
        onChange={(e) => setPath(e.target.value)}
      />
      <button onClick={handleStart}>Start Processing</button>
    </div>
  );
}
