# Generated by Django 4.2.6 on 2023-10-22 18:16

from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='BalCoopModel',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('periodo', models.SmallIntegerField()),
                ('mes', models.SmallIntegerField()),
                ('entidad_nit', models.BigIntegerField()),
                ('puc_codigo', models.CharField(max_length=128)),
                ('saldo', models.DecimalField(decimal_places=2, max_digits=18)),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('updated_at', models.DateTimeField(auto_now_add=True)),
            ],
            options={
                'db_table': 'bal_coop',
                'ordering': ['-created_at'],
            },
        ),
    ]