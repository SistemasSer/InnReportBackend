# Generated by Django 5.0.7 on 2024-07-15 15:14

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('balCoop', '0006_alter_balcoopmodel_entidad_nit'),
    ]

    operations = [
        migrations.AlterModelOptions(
            name='balcoopmodel',
            options={'ordering': ['-updated_at']},
        ),
    ]
