import React from "react";
import { Routes, Route } from "react-router-dom";
import LandingPage from "./pages/LandingPage";
import MainPage from "./pages/MainPage";
import SqlGenerationPage from "./pages/SqlGenerationPage";
import TableLineagePage from "./pages/TableLineagePage";
import ColumnLineagePage from "./pages/ColumnLineagePage";
import MappingGenerationPage from "./pages/MappingGenerationPage";

function App() {
  return (
    <Routes>
      <Route path="/" element={<LandingPage />} />
      <Route path="/app" element={<MainPage />} />
      <Route path="/sql" element={<SqlGenerationPage />} />
      <Route path="/lineage/tables" element={<TableLineagePage />} />
      <Route path="/lineage/columns" element={<ColumnLineagePage />} />
      <Route path="/mapping" element={<MappingGenerationPage />} />
    </Routes>
  );
}

export default App;
