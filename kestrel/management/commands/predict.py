from django.core.management.base import CommandError
from django_rich.management import RichCommand
from rich.progress import MofNCompleteColumn, Progress, TextColumn, TimeElapsedColumn
from skops.io import load

from kestrel.models import SOURCES, Record
from kestrel.util import extract_text_features, get_model_path


class Command(RichCommand):
    help = "Automatically label data as being about procurement."

    def add_arguments(self, parser):
        parser.add_argument("source", choices=SOURCES, help="Source for which to make predictions")
        parser.add_argument("--batch-size", type=int, default=1000, help="Number of records per batch")
        parser.add_argument("--overwrite", action="store_true", help="Re-process records whose predicted_label is set")

    def handle(self, *args, **options):
        source = options["source"]
        batch_size = options["batch_size"]
        overwrite = options["overwrite"]

        path = get_model_path(source)
        if not path.exists():
            raise CommandError(f"Model file not found: {path}. Run the train command to create a model.")

        pipeline = load(path, trusted=["kestrel.management.commands.train.preprocessor"])

        queryset = Record.objects.filter(source=source).order_by("pk")
        if not overwrite:
            queryset = queryset.filter(predicted_label=None)

        positive = 0
        with Progress(
            *Progress.get_default_columns(),
            TimeElapsedColumn(),
            MofNCompleteColumn(),
            TextColumn("({task.fields[positive]} procurement-related)"),
            console=self.console,
        ) as progress:
            task_id = progress.add_task(f"Predicting labels for {source}", total=queryset.count(), positive=0)

            last_pk = 0
            while True:
                records = list(queryset.filter(pk__gt=last_pk)[:batch_size])
                if not records:
                    break

                X = [extract_text_features(source, record.response) for record in records]
                predictions = pipeline.predict(X)
                probabilities = pipeline.predict_proba(X)[:, 1]

                for record, pred, prob in zip(records, predictions, probabilities, strict=True):
                    # Convert from numpy types.
                    record.predicted_label = bool(pred)
                    record.predicted_score = float(prob)
                    positive += int(pred)

                Record.objects.bulk_update(records, ["predicted_label", "predicted_score"])
                progress.update(task_id, advance=len(records), positive=positive)
                last_pk = records[-1].pk
