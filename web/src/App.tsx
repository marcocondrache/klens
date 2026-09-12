import { Outlet } from "@tanstack/react-router";

import { AuthGate } from "@/components/auth-gate";

function App() {
  return (
    <AuthGate>
      <Outlet />
    </AuthGate>
  );
}

export default App;
