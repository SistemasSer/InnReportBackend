# Generated by Django 5.0.7 on 2024-07-12 19:29

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('balCoop', '0004_alter_balcoopmodel_entidad_nit'),
    ]

    operations = [
        migrations.AlterField(
            model_name='balcoopmodel',
            name='entidad_nit',
            field=models.CharField(max_length=64),
        ),
    ]