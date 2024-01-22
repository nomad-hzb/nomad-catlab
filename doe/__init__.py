#
# Copyright The NOMAD Authors.
#
# This file is part of NOMAD. See https://nomad-lab.eu for further info.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os

# from nomad.units import ureg
from nomad.metainfo import (
    Package,
    Section)
from nomad.datamodel.data import EntryData


from baseclasses.design import (
    Design
)

from baseclasses.characterizations import (
    XRR, XRD, XRDData
)

from baseclasses.heterogeneous_catalysis import (
    DesignSample,
    ActiveDesignParameter,
    PassiveDesignParameter,
    FurtherDesignParameter,
    DesignSampleID)

from baseclasses.helper.utilities import get_reference, find_sample_by_id, create_archive, rewrite_json

m_package = Package(name='catlab')


def unnormalize_value(factor, row, lower, upper):
    normal_value = float(row[factor.label])
    min_value = factor.minimum_value
    max_value = factor.maximum_value
    m = (max_value - min_value) / (upper - lower)
    b = (min_value * upper - max_value * lower) / (upper - lower)
    return m * normal_value + b


def visit_factors(
        visited,
        parameters,
        archive,
        factors,
        name,
        row,
        lower=-1,
        upper=1):
    self_reference = get_reference(
        archive.metadata.upload_id, archive.metadata.entry_id)
    for i, factor in enumerate(factors):
        visited.append(factor.label)
        value = factor.value if "value" in factor else unnormalize_value(
            factor, row, lower, upper)
        if "value" in factor:
            next_parameter = PassiveDesignParameter(
                value=value, factor=f"{self_reference}/{name}/{i}")
        else:
            next_parameter = ActiveDesignParameter(
                value=value, factor=f"{self_reference}/{name}/{i}")
        parameters.append(next_parameter)


# %% ####################### Entities


class CatLab_WP4_Sample(DesignSample, EntryData):
    m_def = Section(
        a_eln=dict(
            hide=["users"],
            properties=dict(
                # editable=dict(
                #     exclude=["lab_id"]),
                order=[
                    "name",
                    "lab_id",
                    "chemical_composition_or_formulas",
                    "parent",
                    "desigin",
                    "design_row", "data_files"
                ])),
        label_quantity='sample_id')


class CatLab_WP4_Design(Design, EntryData):
    m_def = Section(
        a_eln=dict(
            hide=["users"],
            properties=dict(
                # editable=dict(
                #     exclude=["lab_id"]),
                order=[
                    "name",
                    "create_design",
                    "lab_id",
                    "chemical_composition_or_formulas",
                    "design_file", "normalized_factor_lower",
                    "normalized_factor_upper"
                ])),
        label_quantity='sample_id')

    def normalize(self, archive, logger):
        super(CatLab_WP4_Design, self).normalize(archive, logger)

        if self.create_design:
            self.create_design = False
            rewrite_json(["data", "create_design"], archive, False)

            with archive.m_context.raw_file(self.design_file, "br") as f:
                import chardet
                encoding = chardet.detect(f.read())["encoding"]

            with archive.m_context.raw_file(self.design_file, encoding=encoding) as f:
                if os.path.splitext(self.design_file)[-1] == ".csv":

                    import pandas as pd

                    design_data = pd.read_csv(f.name, header=0)
                    self_reference = get_reference(
                        archive.metadata.upload_id, archive.metadata.entry_id)
                    for row_index, row in design_data.iterrows():
                        visited = []
                        active_parameters = []
                        if self.active_factors is not None:
                            visit_factors(
                                visited,
                                active_parameters,
                                archive,
                                self.active_factors,
                                "active_factors",
                                row,
                                self.normalized_factor_lower,
                                self.normalized_factor_upper)

                        passive_parameters = []
                        if self.passive_factors is not None:
                            visit_factors(
                                visited,
                                passive_parameters,
                                archive,
                                self.passive_factors,
                                "passive_factors",
                                row)

                        parent = None
                        if "Parent" in design_data.columns:
                            visited.append("Parent")
                            parent = find_sample_by_id(archive, row["Parent"])

                        further_parameters = []
                        for col_name in design_data.columns:
                            if col_name in visited:
                                continue
                            next_parameter = FurtherDesignParameter(
                                value=row[col_name], name=col_name)
                            further_parameters.append(next_parameter)

                        catlab_sample = CatLab_WP4_Sample(
                            chemical_composition_or_formulas=self.chemical_composition_or_formulas,
                            description=self.description,
                            design=self_reference,
                            design_row=row_index,
                            active_parameters=active_parameters,
                            passive_parameters=passive_parameters,
                            parent=parent,
                            further_parameters=further_parameters,
                            sample_id=DesignSampleID(
                                institute=self.design_id.institute,
                                sample_short_name=self.design_id.sample_id,
                                sample_number=row_index,
                            ))

                        file_name = f"{self.design_id.sample_id}_{row_index}.archive.json"
                        create_archive(catlab_sample, archive, file_name)

# %% ### Characterizations


class CatLab_FHI_IRIS_XRR_Brucker(XRR, EntryData):
    m_def = Section(
        a_eln=dict(
            hide=[
                'lab_id',
                'users', "location", "end_time", "metadata_file"],
            properties=dict(
                order=[
                    "name",
                    "data_file",
                    "samples"])),
        a_plot=[
            {
                'x': ['measurement/angle', 'fitted_data/:/angle'],
                'y': ['measurement/intensity', 'fitted_data/:/intensity'],
                'layout': {
                    'yaxis': {
                        "fixedrange": False, 'type': 'log', "title": "Counts"},
                    'xaxis': {
                        "fixedrange": False}}
            },
        ])

    def normalize(self, archive, logger):
        super(CatLab_FHI_IRIS_XRR_Brucker, self).normalize(archive, logger)

        if self.data_file:
            from baseclasses.helper.archive_builder.fhi_archive import get_xrr_data_entry
            measurement, fitted_data = get_xrr_data_entry(
                archive, self.data_file)

            self.measurement = measurement
            self.fitted_data = fitted_data


class CatLab_FHI_IRIS_XRD_Brucker(XRD, EntryData):
    m_def = Section(
        a_eln=dict(
            hide=[
                'lab_id',
                'users', "location", "end_time", "metadata_file"],
            properties=dict(
                order=[
                    "name",
                    "data_file",
                    "samples"])),
        a_plot=[
            {
                'x': ['measurements/:/angle', 'shifted_data/:/angle'],
                'y': ['measurements/:/intensity', 'shifted_data/:/intensity'],
                'layout': {
                    'yaxis': {
                        "fixedrange": False, "title": "Counts"},
                    'xaxis': {
                        "fixedrange": False}}
            },
        ])

    def normalize(self, archive, logger):
        super(CatLab_FHI_IRIS_XRD_Brucker, self).normalize(archive, logger)

        if self.data_file:
            from baseclasses.helper.archive_builder.fhi_archive import get_xrd_data_entry
            measurements, shifted_data = get_xrd_data_entry(
                archive, self.data_file)

            self.measurements = measurements
            self.shifted_data = shifted_data


class CatLab_Wannsee_XRD(XRD, EntryData):
    m_def = Section(
        a_eln=dict(
            hide=[
                'lab_id',
                'users', "location", "end_time", "metadata_file"],
            properties=dict(
                order=[
                    "name",
                    "data_file",
                    "samples"])),
        a_plot=[
            {
                'x': ['measurements/:/angle', 'shifted_data/:/angle'],
                'y': ['measurements/:/intensity', 'shifted_data/:/intensity'],
                'layout': {
                    'yaxis': {
                        "fixedrange": False, "title": "Counts"},
                    'xaxis': {
                        "fixedrange": False}}
            },
        ])

    def normalize(self, archive, logger):
        super(CatLab_Wannsee_XRD, self).normalize(archive, logger)

        if self.data_file:
            measurements = []
            for data_file in self.data_file:
                if os.path.splitext(data_file)[-1] == ".xy":
                    with archive.m_context.raw_file(data_file) as f:
                        import pandas as pd
                        data = pd.read_csv(
                            f.name, skiprows=1, sep=" ", header=None)
                        measurements.append(XRDData(
                            angle_type="2Theta",
                            angle=data[0],
                            intensity=data[1]
                        ))
                self.measurements = measurements
