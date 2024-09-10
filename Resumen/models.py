from django.db import models

class Documento(models.Model):
    id = models.AutoField(primary_key=True) 
    nombre = models.CharField(max_length=255)
    fecha = models.DateField()
    archivo = models.FileField(upload_to='archivo/')

    def __str__(self):
        return self.nombre

