# Generated by Django 5.0.7 on 2024-07-15 21:27

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('balCoop', '0007_alter_balcoopmodel_options'),
    ]

    operations = [
        migrations.RenameField(
            model_name='balcoopmodel',
            old_name='entidad_nit',
            new_name='entidad_RS',
        ),
    ]
