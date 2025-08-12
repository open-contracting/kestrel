import time

import requests
from django.core.management.base import BaseCommand, CommandError
from rich.progress import Progress, TextColumn, TimeElapsedColumn

from kestrel.models import Record


class Command(BaseCommand):
    help = "Collect data from various APIs"

    def add_arguments(self, parser):
        parser.add_argument("source", type=str, choices=["muckrock_foia"], help="Source from which to collect records")
        parser.add_argument("--resume", type=int, help="Resume data collection from this page")

    def handle(self, *args, **options):
        source = options["source"]

        if method := getattr(self, f"collect_{source}", None):
            method(options["resume"])
        else:
            raise CommandError(f"Unknown source: {source}")

    # https://help.muckrock.com/API-19ef8892696381e88627c50e4ee90ed4
    def collect_muckrock_foia(self, resume):
        """Collect FOIA requests from the MuckRock API."""
        page_size = 100
        start_url = (
            f"https://www.muckrock.com/api_v1/foia/?format=json&ordering=-datetime_submitted&page_size={page_size}"
        )
        if resume:
            start_url += f"&page={resume}"

        url = start_url
        inserted = 0
        idle = 0

        with Progress(
            *Progress.get_default_columns(),
            TimeElapsedColumn(),
            TextColumn("{task.completed}/{task.total} records"),
            TextColumn("({task.fields[inserted]} new)"),
            TextColumn("({task.fields[idle]:.1f}s idle)"),
        ) as progress:
            task_id = progress.add_task("Collecting", total=None, inserted=0, idle=0)

            while url:
                progress.console.print(f"GET {url}")
                last_request_time = time.monotonic()

                try:
                    response = requests.get(url, timeout=20)
                    response.raise_for_status()
                except requests.RequestException as e:
                    progress.console.print(e, style="red")
                    break

                data = response.json()

                if progress.tasks[task_id].total is None:
                    progress.update(
                        task_id,
                        total=data["count"],
                        advance=(resume - 1) * page_size if resume else 0,
                    )

                for item in data["results"]:
                    progress.update(task_id, advance=1)
                    external_id = str(item["id"])
                    # Use exists(), because create() always increments the primary key.
                    if not Record.objects.filter(source="muckrock_foia", external_id=external_id).exists():
                        Record.objects.create(source="muckrock_foia", external_id=external_id, response=item)
                        inserted += 1
                        progress.update(task_id, inserted=inserted)

                # The rate limit is 1 request per second.
                if wait := max(0, 1 - (time.monotonic() - last_request_time)):
                    time.sleep(wait)
                    idle += wait
                    progress.update(task_id, idle=idle)

                url = data["next"]
                if not url:
                    break

        self.stdout.write(self.style.SUCCESS(f"{inserted} new records from MuckRock FOIA API (slept {idle:.1f}s)"))
