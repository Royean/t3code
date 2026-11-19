import { AsyncResult, Atom } from "effect/reactivity";
import { sessionDeniesScope } from "@t3tools/client-runtime/state/session";
import { AuthOrchestrationOperateScope, AuthOrchestrationReadScope } from "@t3tools/contracts";

import { environmentPresentations } from "../../state/presentation";
import { serverEnvironment } from "../../state/server";
import { environmentSession, sessionHasScope } from "../../state/session";

export const CHATS_WORKSPACE_TABS = ["threads", "alwaysOn"] as const;
export type ChatsWorkspaceTab = (typeof CHATS_WORKSPACE_TABS)[number];
export const chatsWorkspaceTabAtom = Atom.make<ChatsWorkspaceTab>("threads");

export const alwaysOnWorkspaceEnvironmentsAtom = Atom.make((get) =>
  [...get(environmentPresentations.presentationsAtom)].map(([environmentId, environment]) => {
    const connected = environment.connection.phase === "connected";
    const session = connected ? get(environmentSession.sessionStateAtom(environmentId)) : null;
    const canRead = session !== null && sessionHasScope(session, AuthOrchestrationReadScope);
    const result = canRead
      ? get(serverEnvironment.alwaysOnLive({ environmentId, input: {} }))
      : null;
    return {
      environmentId,
      label: environment.entry.target.label,
      connected,
      canOperate: session !== null && sessionHasScope(session, AuthOrchestrationOperateScope),
      readDenied: session !== null && sessionDeniesScope(session, AuthOrchestrationReadScope),
      result,
      snapshot: result === null ? null : AsyncResult.getOrElse(result, () => null),
    };
  }),
);

export type AlwaysOnWorkspaceEnvironment = Atom.Type<
  typeof alwaysOnWorkspaceEnvironmentsAtom
>[number];
