import React from "react";
import { Button, Stack } from "@mui/material";

export default function MetadataIngestPanel({ onSuccess }) {
  const ingestSample = async () => {
    await fetch("http://localhost:8000/metadata/table", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        layer: "ODP",
        schema: "sales",
        name: "orders_raw",
        type: "source",
        version: 1,
        columns: [
          { name: "order_id", datatype: "int", is_pk: true },
          { name: "customer_id", datatype: "int" }
        ]
      })
    });

    onSuccess();
  };

  return (
    <Stack spacing={2}>
      <Button variant="contained" onClick={ingestSample}>
        Create Sample ODP Table
      </Button>
    </Stack>
  );
}
