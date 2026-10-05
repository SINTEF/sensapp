import { lazy, Suspense } from 'react';
import { Routes, Route } from 'react-router-dom';
import { Layout } from './components/Layout';
import { ExplorerPage } from './pages/ExplorerPage';

// The page that explains how to load data brings the code highlighter: it is fetched when the tab is first opened
const LoadPage = lazy(() => import('./pages/LoadPage'));

function App() {
  return (
    <Routes>
      <Route element={<Layout />}>
        <Route path="/" element={<ExplorerPage />} />
        <Route
          path="/load"
          element={
            <Suspense fallback={null}>
              <LoadPage />
            </Suspense>
          }
        />
      </Route>
    </Routes>
  );
}

export default App;
