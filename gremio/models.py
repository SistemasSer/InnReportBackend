from django.db import models

from entidad.models import EntidadModel

class GremioModel(models.Model):
    nombre = models.CharField(max_length=60)
    descripcion = models.CharField(max_length=60, null=True, blank=True)

    class Meta:
        db_table = "gremio"
        ordering = ['id']
        verbose_name_plural = "gremios"

class GremioToEntity(models.Model):
    Gremio = models.ForeignKey(GremioModel, on_delete=models.CASCADE, related_name='entidades')
    entidad = models.ForeignKey( EntidadModel, on_delete=models.CASCADE,related_name='gremio_to_entity')

    class Meta:
        db_table = "gremio_to_entity"
        unique_together = ('Gremio', 'entidad')