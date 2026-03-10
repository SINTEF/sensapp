import { Routes, Route } from 'react-router-dom';
import { Layout } from './components/Layout';
import { ExplorerPage } from './pages/ExplorerPage';

function App() {
  return (
    <Routes>
      <Route element={<Layout />}>
        <Route path="/" element={<ExplorerPage />} />
      </Route>
    </Routes>
  );
}

export default App;
