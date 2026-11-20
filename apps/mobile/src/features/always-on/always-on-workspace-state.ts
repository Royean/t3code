import { arrayElementsEqual } from "@t3tools/client-runtime/state/entities";
import { AsyncResult, Atom } from "effect/reactivity";
import * as Option from "effect/Option";
import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/shell";
import { sessionDeniesScope } from "@t3tools/client-runtime/state/session";
import {
  AuthOrchestrationOperateScope,
  AuthOrchestrationReadScope,
  AuthSettingsWriteScope,
  isAlwaysOnEventThread,
} from "@t3tools/contracts";

import { environmentPresentations } from "../../state/presentation";
import { serverEnvironment } from "../../state/server";
import { environmentSession, sessionHasScope } from "../../state/session";
import { steadyNavigationThreadShellsAtom } from "../../state/threads";

export const CHATS_WORKSPACE_TABS = ["threads", "alwaysOn"] as const;
export type ChatsWorkspaceTab = (typeof CHATS_WORKSPACE_TABS)[number];
export const chatsWorkspaceTabAtom = Atom.make<ChatsWorkspaceTab>("threads");

function chatsThreadShellsAtom(routed: boolean) {
  return Atom.make((get) => {
    const shells = get(steadyNavigationThreadShellsAtom).filter(
      (thread) => isAlwaysOnEventThread(thread.id) === routed,
    );
    const previous = Option.getOrUndefined(get.self<ReadonlyArray<EnvironmentThreadShell>>());
    return previous && arrayElementsEqual(previous, shells) ? previous : shells;
  });
}

export const ordinaryChatsThreadShellsAtom = chatsThreadShellsAtom(false);
export const alwaysOnEventThreadShellsAtom = chatsThreadShellsAtom(true);

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
      canConfigureRouting: session !== null && sessionHasScope(session, AuthSettingsWriteScope),
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
