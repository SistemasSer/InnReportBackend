# Generated by Django 5.0.7 on 2024-08-02 14:52

from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='Documento',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('nombre', models.CharField(max_length=255)),
                ('fecha', models.DateField()),
                ('archivo', models.FileField(upload_to='archivo/')),
            ],
        ),
    ]