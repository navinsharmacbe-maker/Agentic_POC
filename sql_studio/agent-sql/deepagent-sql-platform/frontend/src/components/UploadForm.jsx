import React, { useState } from "react";
import { uploadZip, startProcessing } from "../services/api";

export default function UploadForm({ onStart }) {
  const [file, setFile] = useState(null);

  const handleUpload = async () => {
    if (!file) return alert("Select a zip file");

    try {
      // 1️⃣ Upload zip
      const res = await uploadZip(file);
      const session_id = res.data.session_id;
      alert("Uploaded! Session ID: " + session_id);

      // 2️⃣ Start processing
      const startRes = await startProcessing({ session_id });
      alert("Processing started: " + startRes.data.status);

      // optional callback
      if (onStart) onStart(session_id);
    } catch (err) {
      console.error(err);
      let msg = "Error uploading or starting processing.";
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
      <input type="file" accept=".zip" onChange={(e) => setFile(e.target.files[0])} />
      <button onClick={handleUpload}>Upload & Start</button>
    </div>
  );
}
