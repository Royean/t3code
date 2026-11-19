import type { AlwaysOnAgentId, AlwaysOnAgentSummary, EnvironmentId } from "@t3tools/contracts";
import * as SecureRandom from "effect/crypto/SecureRandom";
import { useGT } from "gt-react";
import { SendIcon } from "lucide-react";
import { useState } from "react";

import { serverEnvironment } from "../../state/server";
import { useAtomCommand } from "../../state/use-atom-command";
import { Button } from "../ui/button";
import { Textarea } from "../ui/textarea";
import { useAlwaysOnAction } from "./useAlwaysOnAction";

export function AlwaysOnComposer({
  address,
  agent,
  disabled,
  readOnly,
}: {
  readonly address: {
    readonly environmentId: EnvironmentId;
    readonly input: { readonly agentId: AlwaysOnAgentId };
  };
  readonly agent: AlwaysOnAgentSummary | undefined;
  readonly disabled: boolean;
  readonly readOnly: boolean;
}) {
  const gt = useGT();
  const submit = useAtomCommand(serverEnvironment.submitAlwaysOnEvent);
  const { act, error, pending } = useAlwaysOnAction();
  const [draft, setDraft] = useState("");
  const [attempt, setAttempt] = useState<{ readonly text: string; readonly id: string } | null>(
    null,
  );
  const text = draft.trim();

  const send = async () => {
    if (disabled || pending || text === "") return;
    const id = attempt?.text === text ? attempt.id : `chat:${SecureRandom.uuidV4Unsafe()}`;
    setAttempt({ text, id });
    const accepted = await act(() =>
      submit({
        ...address,
        input: { ...address.input, source: "chat", sourceEventId: id, kind: "user_reply", text },
      }),
    );
    if (accepted) {
      setDraft((current) => (current === draft ? "" : current));
      setAttempt(null);
    }
  };

  return (
    <div className="shrink-0 border-t px-4 py-3">
      <form
        aria-busy={pending}
        className="mx-auto w-full max-w-3xl space-y-2"
        onSubmit={(event) => {
          event.preventDefault();
          void send();
        }}
      >
        {agent && agent.pendingInboxCount > 0 ? (
          <p className="text-xs text-muted-foreground" role="status">
            {gt("{count} queued", { count: agent.pendingInboxCount })}
          </p>
        ) : null}
        {readOnly ? (
          <p className="text-xs text-muted-foreground">{gt("Read-only access")}</p>
        ) : null}
        {error ? (
          <p role="alert" className="text-sm text-destructive">
            {error}
          </p>
        ) : null}
        <div className="flex items-end gap-2">
          <Textarea
            aria-label={gt("Message this agent")}
            placeholder={gt("Message this agent")}
            className="min-w-0 flex-1"
            value={draft}
            rows={2}
            maxLength={2_000}
            disabled={disabled}
            onChange={(event) => setDraft(event.target.value)}
            onKeyDown={(event) => {
              if (
                event.key !== "Enter" ||
                event.shiftKey ||
                event.nativeEvent.isComposing ||
                event.keyCode === 229
              )
                return;
              event.preventDefault();
              event.currentTarget.form?.requestSubmit();
            }}
          />
          <Button type="submit" disabled={disabled || pending || text === ""}>
            <SendIcon />
            {gt("Send")}
          </Button>
        </div>
      </form>
    </div>
  );
}
