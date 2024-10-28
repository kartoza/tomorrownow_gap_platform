# Generated by Django 4.2.7 on 2024-10-28 07:24

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('prise', '0003_alter_prisedata_data_type'),
    ]

    operations = [
        migrations.CreateModel(
            name='PriseMessageSchedule',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('group', models.CharField(choices=[('Start of Season', 'Start of Season'), ('PRISE Time To Action 1 - Beginning of the month', 'PRISE Time To Action 1 - Beginning of the month'), ('PRISE Time To Action 2 - Second half of the month', 'PRISE Time To Action 2 - Second half of the month'), ('End of season', 'End of season')], default='Start of Season', max_length=512)),
                ('week_of_month', models.PositiveIntegerField()),
                ('day_of_week', models.PositiveIntegerField()),
                ('schedule_date', models.DateField(blank=True, help_text='Override the schedule date, useful for sending one time message.', null=True)),
                ('active', models.BooleanField(default=True)),
            ],
        ),
    ]
