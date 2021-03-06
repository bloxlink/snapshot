import asyncio
from rethinkdb.errors import ReqlDriverError
from os import environ as env
import datetime
import aiohttp

try:
    from rethinkdb import RethinkDB; r = RethinkDB() # pylint: disable=no-name-in-module
except ImportError:
    import rethinkdb as r

try:
    from config import RETHINKDB
except ImportError:
    RETHINKDB = {
        "HOST": env.get("RETHINKDB_HOST"),
        "PORT": int(env.get("RETHINKDB_PORT")),
        "DB": env.get("RETHINKDB_DB"),
        "PASSWORD": env.get("RETHINKDB_PASSWORD")
    }

GROUP_URL = "https://groups.roblox.com/v1/groups"

r.set_loop_type("asyncio")
loop = asyncio.get_event_loop()


async def get_group_stats(group_id, session=None):
    session = session or aiohttp.ClientSession()

    try:
        async with session.get(f"{GROUP_URL}/{group_id}") as response:
            if response.status == 200:
                json_data = await response.json()
                member_count = json_data.get("memberCount")

                return {
                    "memberCount": member_count
                }

    except aiohttp.client_exceptions.ServerDisconnectedError:
        pass

    return {}

async def main():
    print("Starting snapshot.", flush=True)

    session = aiohttp.ClientSession()
    conn = await r.connect(
        RETHINKDB["HOST"],
        RETHINKDB["PORT"],
        RETHINKDB["DB"],
        password=RETHINKDB["PASSWORD"],
        timeout=5
    )

    conn.repl()

    today = datetime.datetime.today()
    month_name = today.strftime("%B")
    month_day  = str(today.day)
    year  = str(today.year)

    guilds = await r.db("bloxlink").table("guilds").filter(r.row.has_fields("groupIDs")).run()

    async for guild_data in guilds:
        group_ids = guild_data.get("groupIDs", {})

        for group_id, group_data in group_ids.items():
            stats = await get_group_stats(group_id, session=session)

            group_data["stats"] = group_data.get("stats") or {}
            group_data["stats"][year] = group_data["stats"].get(year) or {}
            group_data["stats"][year][month_name] = group_data["stats"][year].get(month_name) or {}
            group_data["stats"][year][month_name][month_day] = stats

            if group_data["stats"].get(month_name): # FIXME: temp
                group_data["stats"].pop(month_name, None)

            group_ids[str(group_id)] = group_data

        guild_data["groupIDs"] = group_ids

        await r.db("bloxlink").table("guilds").insert(guild_data, conflict="replace").run()

    await conn.close()
    await session.close()

    print("Snapshot done.", flush=True)


if __name__ == "__main__":
    try:
        loop.run_until_complete(main())
    finally:
        loop.close()
