import { AuthGate } from "@/components/auth-gate";
import { Outlet } from "@/lib/navigation";

function App() {
  return (
    <AuthGate>
      <Outlet />
    </AuthGate>
  );
}

export default App;
