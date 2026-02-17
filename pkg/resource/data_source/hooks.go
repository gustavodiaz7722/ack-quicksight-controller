// Copyright Amazon.com Inc. or its affiliates. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"). You may
// not use this file except in compliance with the License. A copy of the
// License is located at
//
//     http://aws.amazon.com/apache2.0/
//
// or in the "license" file accompanying this file. This file is distributed
// on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
// express or implied. See the License for the specific language governing
// permissions and limitations under the License.

package data_source

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/quicksight/types"

	ackv1alpha1 "github.com/aws-controllers-k8s/runtime/apis/core/v1alpha1"
	ackrtlog "github.com/aws-controllers-k8s/runtime/pkg/runtime/log"
	acktypes "github.com/aws-controllers-k8s/runtime/pkg/types"

	svcapitypes "github.com/aws-controllers-k8s/quicksight-controller/apis/v1alpha1"
	"github.com/aws-controllers-k8s/quicksight-controller/pkg/tags"
)

// resourceTagManager holds methods for working with AWS resource tags
type resourceTagManager struct {
	// tagManager provides methods for working with AWS resource tags
	tagManager *tags.TagManager
	// logConstructor contains a method that can produce a logger for a
	// resource manager from a supplied context.
	logConstructor func(context.Context) acktypes.Logger
}

// newResourceTagManager returns a new resourceTagManager struct
func newResourceTagManager(
	cfg aws.Config,
	logConstructor func(context.Context) acktypes.Logger,
) *resourceTagManager {
	return &resourceTagManager{
		tagManager:     tags.NewTagManager(cfg, logConstructor),
		logConstructor: logConstructor,
	}
}

// getTags returns the tags for a given resource ARN
func (rtm *resourceTagManager) getTags(
	ctx context.Context,
	resourceARN string,
) []types.Tag {
	tags, err := rtm.tagManager.GetTags(ctx, resourceARN)
	if err != nil {
		rtm.logConstructor(ctx).Debug("error getting tags for resource", "error", err)
		return nil
	}
	return tags
}

// syncTags synchronizes tags between the supplied desired and latest resources
func (rtm *resourceTagManager) syncTags(
	ctx context.Context,
	latest *resource,
	desired *resource,
) error {
	if latest.ko.Status.ACKResourceMetadata == nil || latest.ko.Status.ACKResourceMetadata.ARN == nil {
		return nil
	}
	resourceARN := string(*latest.ko.Status.ACKResourceMetadata.ARN)

	var latestTags []types.Tag
	if latest.ko.Spec.Tags != nil {
		latestTags = make([]types.Tag, len(latest.ko.Spec.Tags))
		for i, tag := range latest.ko.Spec.Tags {
			latestTags[i] = types.Tag{
				Key:   tag.Key,
				Value: tag.Value,
			}
		}
	}

	var desiredTags []types.Tag
	if desired.ko.Spec.Tags != nil {
		desiredTags = make([]types.Tag, len(desired.ko.Spec.Tags))
		for i, tag := range desired.ko.Spec.Tags {
			desiredTags[i] = types.Tag{
				Key:   tag.Key,
				Value: tag.Value,
			}
		}
	}

	_, err := rtm.tagManager.SyncTags(ctx, resourceARN, desiredTags, latestTags)
	if err != nil {
		return err
	}

	// Update the latest resource's tags to match the desired tags
	latest.ko.Spec.Tags = desired.ko.Spec.Tags

	return nil
}

// getTags returns the tags for a given resource ARN
// This is a wrapper method that delegates to resourceTagManager
func (rm *resourceManager) getTags(
	ctx context.Context,
	resourceARN ackv1alpha1.AWSResourceName,
) []*svcapitypes.Tag {
	logConstructor := func(ctx context.Context) acktypes.Logger {
		return ackrtlog.FromContext(ctx)
	}
	rtm := newResourceTagManager(rm.clientcfg, logConstructor)
	sdkTags := rtm.getTags(ctx, string(resourceARN))

	if sdkTags == nil {
		return nil
	}

	// Convert SDK tags to CRD tags
	tags := make([]*svcapitypes.Tag, len(sdkTags))
	for i, tag := range sdkTags {
		tags[i] = &svcapitypes.Tag{
			Key:   tag.Key,
			Value: tag.Value,
		}
	}
	return tags
}

// syncTags synchronizes tags between the supplied desired and latest resources
// This is a wrapper method that delegates to resourceTagManager
func (rm *resourceManager) syncTags(
	ctx context.Context,
	latest *resource,
	desired *resource,
) error {
	logConstructor := func(ctx context.Context) acktypes.Logger {
		return ackrtlog.FromContext(ctx)
	}
	rtm := newResourceTagManager(rm.clientcfg, logConstructor)
	return rtm.syncTags(ctx, latest, desired)
}

// setResourceDataSourceParameters converts SDK DataSourceParameters (union type) to CRD format
func (rm *resourceManager) setResourceDataSourceParameters(sdkParams types.DataSourceParameters) *svcapitypes.DataSourceParameters {
	if sdkParams == nil {
		return nil
	}

	result := &svcapitypes.DataSourceParameters{}

	switch v := sdkParams.(type) {
	case *types.DataSourceParametersMemberS3Parameters:
		s3 := &svcapitypes.S3Parameters{
			RoleARN: v.Value.RoleArn,
		}
		if v.Value.ManifestFileLocation != nil {
			s3.ManifestFileLocation = &svcapitypes.ManifestFileLocation{
				Bucket: v.Value.ManifestFileLocation.Bucket,
				Key:    v.Value.ManifestFileLocation.Key,
			}
		}
		result.S3Parameters = s3

	case *types.DataSourceParametersMemberRedshiftParameters:
		redshift := &svcapitypes.RedshiftParameters{
			ClusterID: v.Value.ClusterId,
			Database:  v.Value.Database,
			Host:      v.Value.Host,
		}
		if v.Value.Port != 0 {
			port := int64(v.Value.Port)
			redshift.Port = &port
		}
		if v.Value.IAMParameters != nil {
			redshift.IAMParameters = &svcapitypes.RedshiftIAMParameters{
				RoleARN:                v.Value.IAMParameters.RoleArn,
				DatabaseUser:           v.Value.IAMParameters.DatabaseUser,
				AutoCreateDatabaseUser: &v.Value.IAMParameters.AutoCreateDatabaseUser,
			}
			if len(v.Value.IAMParameters.DatabaseGroups) > 0 {
				groups := make([]*string, len(v.Value.IAMParameters.DatabaseGroups))
				for i, g := range v.Value.IAMParameters.DatabaseGroups {
					groups[i] = aws.String(g)
				}
				redshift.IAMParameters.DatabaseGroups = groups
			}
		}
		if v.Value.IdentityCenterConfiguration != nil {
			redshift.IdentityCenterConfiguration = &svcapitypes.IdentityCenterConfiguration{
				EnableIdentityPropagation: v.Value.IdentityCenterConfiguration.EnableIdentityPropagation,
			}
		}
		result.RedshiftParameters = redshift

	case *types.DataSourceParametersMemberAthenaParameters:
		athena := &svcapitypes.AthenaParameters{
			WorkGroup: v.Value.WorkGroup,
			RoleARN:   v.Value.RoleArn,
		}
		if v.Value.IdentityCenterConfiguration != nil {
			athena.IdentityCenterConfiguration = &svcapitypes.IdentityCenterConfiguration{
				EnableIdentityPropagation: v.Value.IdentityCenterConfiguration.EnableIdentityPropagation,
			}
		}
		result.AthenaParameters = athena

	case *types.DataSourceParametersMemberRdsParameters:
		result.RdsParameters = &svcapitypes.RdsParameters{
			Database:   v.Value.Database,
			InstanceID: v.Value.InstanceId,
		}

	case *types.DataSourceParametersMemberAmazonElasticsearchParameters:
		result.AmazonElasticsearchParameters = &svcapitypes.AmazonElasticsearchParameters{
			Domain: v.Value.Domain,
		}

	case *types.DataSourceParametersMemberAmazonOpenSearchParameters:
		result.AmazonOpenSearchParameters = &svcapitypes.AmazonOpenSearchParameters{
			Domain: v.Value.Domain,
		}

	case *types.DataSourceParametersMemberAuroraParameters:
		result.AuroraParameters = &svcapitypes.AuroraParameters{
			Database: v.Value.Database,
			Host:     v.Value.Host,
			Port:     convertInt32PtrToInt64Ptr(v.Value.Port),
		}

	case *types.DataSourceParametersMemberAuroraPostgreSqlParameters:
		result.AuroraPostgreSQLParameters = &svcapitypes.AuroraPostgreSQLParameters{
			Database: v.Value.Database,
			Host:     v.Value.Host,
			Port:     convertInt32PtrToInt64Ptr(v.Value.Port),
		}

	case *types.DataSourceParametersMemberAwsIotAnalyticsParameters:
		result.AWSIOtAnalyticsParameters = &svcapitypes.AWSIOtAnalyticsParameters{
			DataSetName: v.Value.DataSetName,
		}

	case *types.DataSourceParametersMemberBigQueryParameters:
		result.BigQueryParameters = &svcapitypes.BigQueryParameters{
			ProjectID:     v.Value.ProjectId,
			DataSetRegion: v.Value.DataSetRegion,
		}

	case *types.DataSourceParametersMemberConfluenceParameters:
		result.ConfluenceParameters = &svcapitypes.ConfluenceParameters{
			ConfluenceURL: v.Value.ConfluenceUrl,
		}

	case *types.DataSourceParametersMemberCustomConnectionParameters:
		result.CustomConnectionParameters = &svcapitypes.CustomConnectionParameters{
			ConnectionType: v.Value.ConnectionType,
		}

	case *types.DataSourceParametersMemberDatabricksParameters:
		result.DatabricksParameters = &svcapitypes.DatabricksParameters{
			Host:            v.Value.Host,
			Port:            convertInt32PtrToInt64Ptr(v.Value.Port),
			SQLEndpointPath: v.Value.SqlEndpointPath,
		}

	case *types.DataSourceParametersMemberExasolParameters:
		result.ExasolParameters = &svcapitypes.ExasolParameters{
			Host: v.Value.Host,
			Port: convertInt32PtrToInt64Ptr(v.Value.Port),
		}

	case *types.DataSourceParametersMemberImpalaParameters:
		result.ImpalaParameters = &svcapitypes.ImpalaParameters{
			Database:        v.Value.Database,
			Host:            v.Value.Host,
			Port:            convertInt32PtrToInt64Ptr(v.Value.Port),
			SQLEndpointPath: v.Value.SqlEndpointPath,
		}

	case *types.DataSourceParametersMemberJiraParameters:
		result.JiraParameters = &svcapitypes.JiraParameters{
			SiteBaseURL: v.Value.SiteBaseUrl,
		}

	case *types.DataSourceParametersMemberMariaDbParameters:
		result.MariaDBParameters = &svcapitypes.MariaDBParameters{
			Database: v.Value.Database,
			Host:     v.Value.Host,
			Port:     convertInt32PtrToInt64Ptr(v.Value.Port),
		}

	case *types.DataSourceParametersMemberMySqlParameters:
		result.MySQLParameters = &svcapitypes.MySQLParameters{
			Database: v.Value.Database,
			Host:     v.Value.Host,
			Port:     convertInt32PtrToInt64Ptr(v.Value.Port),
		}

	case *types.DataSourceParametersMemberOracleParameters:
		result.OracleParameters = &svcapitypes.OracleParameters{
			Database:       v.Value.Database,
			Host:           v.Value.Host,
			Port:           convertInt32PtrToInt64Ptr(v.Value.Port),
			UseServiceName: &v.Value.UseServiceName,
		}

	case *types.DataSourceParametersMemberPostgreSqlParameters:
		result.PostgreSQLParameters = &svcapitypes.PostgreSQLParameters{
			Database: v.Value.Database,
			Host:     v.Value.Host,
			Port:     convertInt32PtrToInt64Ptr(v.Value.Port),
		}

	case *types.DataSourceParametersMemberPrestoParameters:
		result.PrestoParameters = &svcapitypes.PrestoParameters{
			Catalog: v.Value.Catalog,
			Host:    v.Value.Host,
			Port:    convertInt32PtrToInt64Ptr(v.Value.Port),
		}

	case *types.DataSourceParametersMemberQBusinessParameters:
		result.QBusinessParameters = &svcapitypes.QBusinessParameters{
			ApplicationARN: v.Value.ApplicationArn,
		}

	case *types.DataSourceParametersMemberS3KnowledgeBaseParameters:
		result.S3KnowledgeBaseParameters = &svcapitypes.S3KnowledgeBaseParameters{
			BucketURL:             v.Value.BucketUrl,
			MetadataFilesLocation: v.Value.MetadataFilesLocation,
			RoleARN:               v.Value.RoleArn,
		}

	case *types.DataSourceParametersMemberServiceNowParameters:
		result.ServiceNowParameters = &svcapitypes.ServiceNowParameters{
			SiteBaseURL: v.Value.SiteBaseUrl,
		}

	case *types.DataSourceParametersMemberSnowflakeParameters:
		result.SnowflakeParameters = &svcapitypes.SnowflakeParameters{
			Database:  v.Value.Database,
			Host:      v.Value.Host,
			Warehouse: v.Value.Warehouse,
		}

	case *types.DataSourceParametersMemberSparkParameters:
		result.SparkParameters = &svcapitypes.SparkParameters{
			Host: v.Value.Host,
			Port: convertInt32PtrToInt64Ptr(v.Value.Port),
		}

	case *types.DataSourceParametersMemberSqlServerParameters:
		result.SQLServerParameters = &svcapitypes.SQLServerParameters{
			Database: v.Value.Database,
			Host:     v.Value.Host,
			Port:     convertInt32PtrToInt64Ptr(v.Value.Port),
		}

	case *types.DataSourceParametersMemberStarburstParameters:
		result.StarburstParameters = &svcapitypes.StarburstParameters{
			Catalog: v.Value.Catalog,
			Host:    v.Value.Host,
			Port:    convertInt32PtrToInt64Ptr(v.Value.Port),
		}

	case *types.DataSourceParametersMemberTeradataParameters:
		result.TeradataParameters = &svcapitypes.TeradataParameters{
			Database: v.Value.Database,
			Host:     v.Value.Host,
			Port:     convertInt32PtrToInt64Ptr(v.Value.Port),
		}

	case *types.DataSourceParametersMemberTrinoParameters:
		result.TrinoParameters = &svcapitypes.TrinoParameters{
			Catalog: v.Value.Catalog,
			Host:    v.Value.Host,
			Port:    convertInt32PtrToInt64Ptr(v.Value.Port),
		}

	case *types.DataSourceParametersMemberTwitterParameters:
		result.TwitterParameters = &svcapitypes.TwitterParameters{
			MaxRows: convertInt32PtrToInt64Ptr(v.Value.MaxRows),
			Query:   v.Value.Query,
		}
	}

	return result
}

// convertInt32PtrToInt64Ptr converts *int32 to *int64
func convertInt32PtrToInt64Ptr(val *int32) *int64 {
	if val == nil {
		return nil
	}
	result := int64(*val)
	return &result
}

// setResourceSSLProperties converts SDK SslProperties to CRD format
func (rm *resourceManager) setResourceSSLProperties(sdkProps *types.SslProperties) *svcapitypes.SSLProperties {
	if sdkProps == nil {
		return nil
	}

	return &svcapitypes.SSLProperties{
		DisableSSL: &sdkProps.DisableSsl,
	}
}

// setResourceVPCConnectionProperties converts SDK VpcConnectionProperties to CRD format
func (rm *resourceManager) setResourceVPCConnectionProperties(sdkProps *types.VpcConnectionProperties) *svcapitypes.VPCConnectionProperties {
	if sdkProps == nil {
		return nil
	}

	return &svcapitypes.VPCConnectionProperties{
		VPCConnectionARN: sdkProps.VpcConnectionArn,
	}
}
