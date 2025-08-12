from django.db import models


class Record(models.Model):
    source = models.TextField()
    external_id = models.TextField()
    response = models.JSONField(default=dict)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        constraints = [models.UniqueConstraint(fields=["source", "external_id"], name="unique_source_external_id")]

    def __str__(self):
        return f"{self.source}:{self.external_id}"
