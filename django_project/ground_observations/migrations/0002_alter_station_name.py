# Generated by Django 4.2.7 on 2024-06-23 21:46

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('ground_observations', '0001_initial'),
    ]

    operations = [
        migrations.AlterField(
            model_name='station',
            name='name',
            field=models.TextField(),
        ),
    ]
