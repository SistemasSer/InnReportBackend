# Generated by Django 5.0.7 on 2025-01-15 14:19

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('pucSup', '0002_alter_pucsupmodel_options'),
    ]

    operations = [
        migrations.AlterField(
            model_name='pucsupmodel',
            name='created_at',
            field=models.DateTimeField(auto_now_add=True, null=True),
        ),
        migrations.AlterField(
            model_name='pucsupmodel',
            name='updated_at',
            field=models.DateTimeField(auto_now=True, null=True),
        ),
    ]
