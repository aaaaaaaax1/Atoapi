import { GraphitePrototypeHost } from "./GraphitePrototypeHost";
import { useGraphiteControlPlane } from "./useGraphiteControlPlane";

export default function App() {
  const controlPlane = useGraphiteControlPlane();
  return <GraphitePrototypeHost {...controlPlane} />;
}
