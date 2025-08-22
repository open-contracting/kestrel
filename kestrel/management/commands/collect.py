import time
from pathlib import Path
from urllib.parse import urlparse

import requests
from django.core.management.base import CommandError
from django_rich.management import RichCommand
from rich.progress import MofNCompleteColumn, Progress, TextColumn, TimeElapsedColumn

from kestrel.models import SOURCES, Record


class Command(RichCommand):
    help = "Collect data from various APIs"

    def make_rich_console(self, **kwargs):
        return super().make_rich_console(**kwargs, highlight=False)  # disable distracting number highlights

    def add_arguments(self, parser):
        parser.add_argument("source", choices=SOURCES, help="Source from which to collect data")
        parser.add_argument("--resume", type=int, metavar="PAGE", help="Resume data collection from this page")
        parser.add_argument("--limit", type=int, help="Maximum number of items to collect")

    def handle(self, *args, **options):
        source = options["source"]

        if method := getattr(self, f"collect_{source}", None):
            method(options)
        else:
            raise CommandError(f"Unknown source: {source}")

    # https://help.muckrock.com/API-19ef8892696381e88627c50e4ee90ed4
    def collect_muckrock_foia(self, options):
        """Collect FOIA requests from the MuckRock API."""
        resume = options["resume"]
        page_size = 100

        start_url = (
            f"https://www.muckrock.com/api_v1/foia/?format=json&ordering=-datetime_submitted&page_size={page_size}"
        )
        if resume:
            start_url += f"&page={resume}"

        url = start_url
        inserted = 0
        updated = 0
        idle = 0

        with Progress(
            *Progress.get_default_columns(),
            TimeElapsedColumn(),
            MofNCompleteColumn(),
            TextColumn("({task.fields[inserted]} new, {task.fields[updated]} updated)"),
            TextColumn("({task.fields[idle]:.1f}s idle)"),
            console=self.console,
        ) as progress:
            task_id = progress.add_task(
                "Collecting records from muckrock_foia", total=None, inserted=0, updated=0, idle=0
            )

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
                    external_id = str(item["id"])
                    _, created = Record.objects.update_or_create(
                        source="muckrock_foia", external_id=external_id, defaults={"response": item}
                    )
                    if created:
                        inserted += 1
                        progress.update(task_id, inserted=inserted)
                    else:
                        updated += 1
                        progress.update(task_id, updated=updated)
                    progress.advance(task_id)

                # The rate limit is 1 request per second.
                if wait := max(0, 1 - (time.monotonic() - last_request_time)):
                    time.sleep(wait)
                    idle += wait
                    progress.update(task_id, idle=idle)

                url = data["next"]
                if not url:
                    break

    def collect_muckrock_foia_files(self, options):
        """Download files from FOIA requests predicted to be procurement-related."""
        queryset = Record.objects.filter(source="muckrock_foia", predicted_label=True).order_by("pk")
        if limit := options["limit"]:
            queryset = queryset[:limit]

        output_dir = Path("downloads")
        downloaded = 0
        skipped = 0

        with Progress(
            *Progress.get_default_columns(),
            TimeElapsedColumn(),
            MofNCompleteColumn(),
            TextColumn("({task.fields[downloaded]} downloaded, {task.fields[skipped]} skipped)"),
            console=self.console,
        ) as progress:
            task_id = progress.add_task(
                "Downloading files from muckrock_foia", total=queryset.count(), downloaded=0, skipped=0
            )

            for record in queryset:
                response = record.response
                for communication in response["communications"]:
                    # TODO(james): Handle simple cases for now.
                    # https://github.com/open-contracting/kestrel/issues/5
                    if communication["status"] != "done":
                        continue

                    for file in communication["files"]:
                        url = file["ffile"]
                        # TODO(james): Handle simple cases for now.
                        # https://github.com/open-contracting/kestrel/issues/5
                        if not url or not file["pages"]:
                            continue

                        directory = output_dir / f"record_{record.pk}"
                        directory.mkdir(exist_ok=True)
                        path = directory / urlparse(url).path.rsplit("/", 1)[-1]
                        if path.exists():
                            skipped += 1
                            progress.update(task_id, skipped=skipped)
                            continue

                        with requests.get(url, timeout=20, stream=True) as response:
                            response.raise_for_status()
                            with path.open("wb") as f:
                                for chunk in response.iter_content(chunk_size=8192):
                                    f.write(chunk)

                        downloaded += 1
                        progress.update(task_id, downloaded=downloaded)
                        progress.console.print(f"Downloaded {path}")

                progress.advance(task_id)
