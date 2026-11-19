import type { AtomCommandResult } from "@t3tools/client-runtime/state/runtime";
import { useMessages } from "gt-react";
import { useState } from "react";

import { formatEnvironmentQueryError } from "../../state/query";

export function useAlwaysOnAction() {
  const resolve = useMessages();
  const [pending, setPending] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const act = async (run: () => Promise<AtomCommandResult<unknown, unknown>>) => {
    if (pending) return false;
    setPending(true);
    setError(null);
    const result = await run();
    setPending(false);
    if (result._tag === "Failure") {
      setError(formatEnvironmentQueryError(result.cause, resolve));
      return false;
    }
    return true;
  };

  return { act, error, pending };
}
