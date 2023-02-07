**URL**: `/admin/reinit`

**Method**: `POST`

**Query parameters**:
 - `wipe=true|false`: optional, true if kafkakewl should purge all persisted state (by default false)

**Be extra careful, you can lose all your state!!!**

**Description**: **FOR DEVELOPMENT ONLY!!!** re-initializes kafkakewl from the persistence storage with or without purging all persisted state

If `wipe` = `false`, it re-loads everything from the persisted storage (same as bouncing but without bouncing). If `wipe` = `true`, it purges the persisted storage, then re-loads kafkakewl with empty state.

**It's useful for development only.**

**Response**: either [`Failed`](../Failed.md) or [`Succeeded`](../Succeeded.md). In case of [`Succeeded`](../Succeeded.md), the `response` field is null.
