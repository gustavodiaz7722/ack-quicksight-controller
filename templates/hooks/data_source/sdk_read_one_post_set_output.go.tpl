{{/* 
  This hook is called after setting the output fields from the DescribeDataSource API call.
  It retrieves the tags for the resource and sets them in the Spec.Tags field.
  It also sets the Name field from the DataSource object in the response.
*/}}
if resp.DataSource != nil {
	if resp.DataSource.Name != nil {
		ko.Spec.Name = resp.DataSource.Name
	}
	if resp.DataSource.Type != "" {
		typeStr := string(resp.DataSource.Type)
		ko.Spec.Type = &typeStr
	}
	if resp.DataSource.DataSourceParameters != nil {
		ko.Spec.DataSourceParameters = rm.setResourceDataSourceParameters(resp.DataSource.DataSourceParameters)
	}
	if resp.DataSource.SslProperties != nil {
		ko.Spec.SSLProperties = rm.setResourceSSLProperties(resp.DataSource.SslProperties)
	}
	if resp.DataSource.VpcConnectionProperties != nil {
		ko.Spec.VPCConnectionProperties = rm.setResourceVPCConnectionProperties(resp.DataSource.VpcConnectionProperties)
	}
}
if ko.Status.ACKResourceMetadata != nil && ko.Status.ACKResourceMetadata.ARN != nil {
    ko.Spec.Tags = rm.getTags(ctx, *ko.Status.ACKResourceMetadata.ARN)
}