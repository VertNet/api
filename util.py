# This file is part of VertNet: https://github.com/VertNet/webapp
#
# VertNet is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# VertNet is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with VertNet.  If not, see: http://www.gnu.org/licenses

"""Common utilities."""

LAST_UPDATED = '2016-05-20T10:11:43+CEST'

ADD_TO_DOWNLOAD_RESULTS = ['url', 'gbifdatasetid', 'gbifpublisherid', 'email',
                           'contact', 'migrator', 'pubdate', 'lastindexed',
                           'iptlicense']

OMIT_FROM_DOWNLOADS = ['location', 'record', 'verbatim_record', 'count',
                       'icode', 'harvestid', 'eml', 'dwca', 'title',
                       'description', 'orgname', 'emlrights', 'citation',
                       'networks', 'hashid', 'rank', 'haslicense', 'id',
                       'media', 'tissue', 'mappable', 'wascaptive',
                       'source_url', 'fossil', 'hastypestatus', 'type',
                       'language', 'iptrecordid']

TRANSLATE_HEADER = {'pubdate': 'dataset_pubdate',
                    'url': 'dataset_url',
                    'eml': 'dataset_eml',
                    'dwca': 'dataset_dwca',
                    'title': 'dataset_title',
                    'description': 'dataset_description',
                    'contact': 'dataset_contact',
                    'orgname': 'dataset_orgname',
                    'email': 'dataset_contact_email',
                    'emlrights': 'dataset_rights',
                    'citation': 'dataset_citation',
                    'networks': 'dataset_networks',
                    'dctype': 'type',
                    'migrator': 'migrator_version'}

DWC_RECLEVEL = ['Type', 'Modified', 'Language', 'License', 'RightsHolder',
                'AccessRights', 'BibliographicCitation', 'References',
                'InstitutionID', 'CollectionID', 'DatasetID',
                'InstitutionCode', 'CollectionCode', 'DatasetName',
                'OwnerInstitutionCode', 'BasisOfRecord', 'InformationWithheld',
                'DataGeneralizations', 'DynamicProperties']

DWC_OCC = ['OccurrenceID', 'CatalogNumber', 'RecordNumber', 'RecordedBy',
           'IndividualCount', 'OrganismQuantity', 'OrganismQuantityType',
           'Sex', 'LifeStage', 'ReproductiveCondition',
           'Behavior', 'EstablishmentMeans', 'OccurrenceStatus',
           'Preparations', 'Disposition', 'AssociatedMedia',
           'AssociatedReferences', 'AssociatedSequences', 'AssociatedTaxa',
           'OtherCatalogNumbers', 'OccurrenceRemarks']

DWC_ORGANISM = ['OrganismID', 'OrganismName', 'OrganismScope',
                'AssociatedOccurrences', 'AssociatedOrganisms',
                'PreviousIdentifications', 'OrganismRemarks']

DWC_SAMPLE = ['MaterialSampleID']

DWC_EVENT = ['EventID', 'parentEventID', 'FieldNumber', 'EventDate',
             'EventTime', 'StartDayOfYear', 'EndDayOfYear', 'Year', 'Month',
             'Day', 'VerbatimEventDate', 'Habitat', 'SamplingProtocol',
             'SampleSizeValue', 'SampleSizeUnit', 'SamplingEffort',
             'FieldNotes', 'EventRemarks']

DWC_LOCATION = ['LocationID', 'HigherGeographyID', 'HigherGeography',
                'Continent', 'WaterBody', 'IslandGroup', 'Island', 'Country',
                'CountryCode', 'StateProvince', 'County', 'Municipality',
                'Locality', 'VerbatimLocality', 'MinimumElevationInMeters',
                'MaximumElevationInMeters', 'VerbatimElevation',
                'MinimumDepthInMeters', 'MaximumDepthInMeters',
                'VerbatimDepth', 'MinimumDistanceAboveSurfaceInMeters',
                'MaximumDistanceAboveSurfaceInMeters', 'LocationAccordingTo',
                'LocationRemarks', 'DecimalLatitude', 'DecimalLongitude',
                'GeodeticDatum', 'CoordinateUncertaintyInMeters',
                'CoordinatePrecision', 'PointRadiusSpatialFit',
                'VerbatimCoordinates', 'VerbatimLatitude',
                'VerbatimLongitude', 'VerbatimCoordinateSystem', 'VerbatimSRS',
                'FootprintWKT', 'FootprintSRS', 'FootprintSpatialFit',
                'GeoreferencedBy', 'GeoreferencedDate', 'GeoreferenceProtocol',
                'GeoreferenceSources', 'GeoreferenceVerificationStatus',
                'GeoreferenceRemarks']

DWC_GEO = ['GeologicalContextID', 'EarliestEonOrLowestEonothem',
           'LatestEonOrHighestEonothem', 'EarliestEraOrLowestErathem',
           'LatestEraOrHighestErathem', 'EarliestPeriodOrLowestSystem',
           'LatestPeriodOrHighestSystem', 'EarliestEpochOrLowestSeries',
           'LatestEpochOrHighestSeries', 'EarliestAgeOrLowestStage',
           'LatestAgeOrHighestStage', 'LowestBiostratigraphicZone',
           'HighestBiostratigraphicZone', 'LithostratigraphicTerms', 'Group',
           'Formation', 'Member', 'Bed']

DWC_ID = ['IdentificationID', 'IdentificationQualifier', 'TypeStatus',
          'IdentifiedBy', 'DateIdentified', 'IdentificationReferences',
          'IdentificationVerificationStatus', 'IdentificationRemarks']

DWC_TAXON = ['TaxonID', 'ScientificNameID', 'AcceptedNameUsageID',
             'ParentNameUsageID', 'OriginalNameUsageID', 'NameAccordingToID',
             'NamePublishedInID', 'TaxonConceptID', 'ScientificName',
             'AcceptedNameUsage', 'ParentNameUsage', 'OriginalNameUsage',
             'NameAccordingTo', 'NamePublishedIn', 'NamePublishedInYear',
             'HigherClassification', 'Kingdom', 'Phylum', 'Class', 'Order',
             'Family', 'Genus', 'Subgenus', 'SpecificEpithet',
             'InfraspecificEpithet', 'TaxonRank', 'VerbatimTaxonRank',
             'ScientificNameAuthorship', 'VernacularName', 'NomenclaturalCode',
             'TaxonomicStatus', 'NomenclaturalStatus', 'TaxonRemarks']

DWC_ALL = DWC_RECLEVEL + DWC_OCC + DWC_ORGANISM + DWC_SAMPLE + DWC_EVENT \
    + DWC_LOCATION + DWC_GEO + DWC_ID + DWC_TAXON
DWC_ALL_LOWER = [x.lower() for x in DWC_ALL]
DWC_HEADER_LIST = DWC_ALL_LOWER + ADD_TO_DOWNLOAD_RESULTS
DWC_HEADER = '\t'.join(DWC_HEADER_LIST)


def format_json(json_doc):
    return json_doc.replace('""{', '{').replace('}""', '}').replace('""', '"')


def download_field_list():
    """Create a list of the fields in a download.
These are the original field names."""
    field_list = DWC_HEADER_LIST
    for f in OMIT_FROM_DOWNLOADS:
        try:
            field_list.remove(f)
        except:
            pass
    return field_list


def download_header():
    """Create a list of the fields in a download.
These are the original field names."""
    fieldlist = download_field_list()
    i = 0
    for f in fieldlist:
        if f in TRANSLATE_HEADER:
            fieldlist[i] = TRANSLATE_HEADER[f]
        i = i+1
    return '\t'.join(fieldlist)


def classify(record):
    result = dict(meta=record, reclevel={}, occ={}, organism={}, sample={},
                  event={}, loc={}, geo={}, id={}, taxon={})

    for term in DWC_RECLEVEL:
        if term.lower() in record:
            result['reclevel'][term] = record[term.lower()]

    for term in DWC_OCC:
        if term.lower() in record:
            result['occ'][term] = record[term.lower()]

    for term in DWC_ORGANISM:
        if term.lower() in record:
            result['organism'][term] = record[term.lower()]

    for term in DWC_SAMPLE:
        if term.lower() in record:
            result['sample'][term] = record[term.lower()]

    for term in DWC_EVENT:
        if term.lower() in record:
            result['event'][term] = record[term.lower()]

    for term in DWC_LOCATION:
        if term.lower() in record:
            result['loc'][term] = record[term.lower()]

    for term in DWC_GEO:
        if term.lower() in record:
            result['geo'][term] = record[term.lower()]

    for term in DWC_ID:
        if term.lower() in record:
            result['id'][term] = record[term.lower()]

    for term in DWC_TAXON:
        if term.lower() in record:
            result['taxon'][term] = record[term.lower()]

    return result


def search_resource_counts(recs, old_res_counts=None):
    # Build dictionary of resources with their record counts
    res_counts = {}
    gbifdatasetid = None
    for rec in recs:
        if 'gbifdatasetid' in rec:
            gbifdatasetid = rec['gbifdatasetid']
        else:
            gbifdatasetid = rec[TRANSLATE_HEADER['gbifdatasetid']]
        if gbifdatasetid not in res_counts:
            res_counts[gbifdatasetid] = 1
        else:
            res_counts[gbifdatasetid] += 1
    return res_counts


def _tsv(json_doc):
    # These should be the names of the original fields in the index document.
    download_fields = download_field_list()
    values = []
    for x in download_fields:
        if x in json_doc:
            if x == 'dynamicproperties':
                dp = format_json(json_doc[x])
                values.append(unicode(dp.rstrip()))
            else:
                values.append(unicode(json_doc[x]).rstrip())
        else:
            values.append(u'')
    return u'\t'.join(values).encode('utf-8')


def _get_tsv_chunk(records):
    if records is None or len(records) == 0:
        return '\n'
    tsv_lines = map(_tsv, records)
    chunk = reduce(lambda x, y: '%s\n%s' % (x, y), tsv_lines)
    return chunk
