# Generated by Django 5.0.7 on 2024-07-22 20:08

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('balSup', '0002_alter_balsupmodel_entidad_rs'),
    ]

    operations = [
        migrations.AlterField(
            model_name='balsupmodel',
            name='entidad_RS',
            field=models.CharField(max_length=150),
        ),
    ]