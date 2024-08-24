# Generated by Django 4.2.7 on 2024-08-24 09:18

from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion
import uuid


class Migration(migrations.Migration):

    initial = True

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name='BackgroundTask',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('status', models.CharField(choices=[('Pending', 'Pending'), ('Queued', 'Queued'), ('Running', 'Running'), ('Stopped', 'Stopped with error'), ('Completed', 'Completed'), ('Cancelled', 'Cancelled'), ('Invalidated', 'Invalidated')], default='Pending', max_length=255)),
                ('task_name', models.CharField(blank=True, max_length=255, null=True)),
                ('task_id', models.CharField(blank=True, max_length=256, null=True)),
                ('uuid', models.UUIDField(default=uuid.uuid4, unique=True)),
                ('submitted_on', models.DateTimeField()),
                ('started_at', models.DateTimeField(blank=True, null=True)),
                ('finished_at', models.DateTimeField(blank=True, null=True)),
                ('errors', models.TextField(blank=True, null=True)),
                ('stack_trace_errors', models.TextField(blank=True, null=True)),
                ('progress', models.FloatField(blank=True, null=True)),
                ('progress_text', models.TextField(blank=True, null=True)),
                ('last_update', models.DateTimeField(blank=True, null=True)),
                ('parameters', models.TextField(blank=True, null=True)),
                ('context_id', models.CharField(blank=True, max_length=255, null=True)),
                ('submitted_by', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to=settings.AUTH_USER_MODEL)),
            ],
        ),
    ]
