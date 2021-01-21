/**
 * Copyright © 2019 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.google.cloud;

import com.github.jcustenborder.kafka.connect.utils.config.ConfigKeyBuilder;
import com.github.jcustenborder.kafka.connect.utils.config.ConfigUtils;
import com.github.jcustenborder.kafka.connect.utils.config.Description;
import com.github.jcustenborder.kafka.connect.utils.config.recommenders.Recommenders;
import com.github.jcustenborder.kafka.connect.utils.config.validators.Validators;
import com.github.jcustenborder.kafka.connect.utils.config.validators.filesystem.ValidFile;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.io.File;
import java.util.Map;

public abstract class GoogleCloudConnectorConfig extends AbstractConfig {
  public final CredentialLocation credentialLocation;
  public final File credentialPath;
  public final String defaultResourceType;

  public enum CredentialLocation {
    @Description("The default credentials will be used. See GoogleCredentials.getApplicationDefault().")
    Default,
    @Description("The default credentials will be used. See GoogleCredentials.getApplicationDefault().")
    JsonFile
  }

  public static final String GOOGLE_CREDENTIAL_LOCATION_CONFIG = "google.credential.location";
  static final String GOOGLE_CREDENTIAL_LOCATION_DOC = "The location to look for the credentials to connect to the Google Services. " + ConfigUtils.enumDescription(CredentialLocation.class);


  public static final String GOOGLE_CREDENTIAL_PATH_CONFIG = "google.credential.path";
  static final String GOOGLE_CREDENTIAL_PATH_DOC = "";
  public static final String GOOGLE_LOGGING_DEFAULT_RESOURCE_TYPE_CONFIG = "google.logging.default.resource.type";
  static final String GOOGLE_LOGGING_DEFAULT_RESOURCE_TYPE_DOC = "";


  public GoogleCloudConnectorConfig(ConfigDef configDef, Map<?, ?> originals) {
    super(configDef, originals);
    this.credentialLocation = ConfigUtils.getEnum(CredentialLocation.class, this, GOOGLE_CREDENTIAL_LOCATION_CONFIG);
    this.credentialPath = (CredentialLocation.JsonFile == this.credentialLocation) ?
        ConfigUtils.getAbsoluteFile(this, GOOGLE_CREDENTIAL_PATH_CONFIG) :
        null;
    this.defaultResourceType = getString(GOOGLE_LOGGING_DEFAULT_RESOURCE_TYPE_CONFIG);
  }

  public static ConfigDef config() {
    return new ConfigDef()
        .define(
            ConfigKeyBuilder.of(GOOGLE_CREDENTIAL_LOCATION_CONFIG, ConfigDef.Type.STRING)
                .importance(ConfigDef.Importance.HIGH)
                .documentation(GOOGLE_CREDENTIAL_LOCATION_DOC)
                .importance(ConfigDef.Importance.HIGH)
                .validator(Validators.validEnum(CredentialLocation.class))
                .recommender(Recommenders.enumValues(CredentialLocation.class))
                .defaultValue(CredentialLocation.Default.toString())
                .build()
        ).define(
            ConfigKeyBuilder.of(GOOGLE_CREDENTIAL_PATH_CONFIG, ConfigDef.Type.STRING)
                .importance(ConfigDef.Importance.HIGH)
                .documentation(GOOGLE_CREDENTIAL_PATH_DOC)
                .importance(ConfigDef.Importance.HIGH)
                .validator(Validators.blankOr(ValidFile.of()))
                .recommender(Recommenders.visibleIf(GOOGLE_CREDENTIAL_LOCATION_CONFIG, CredentialLocation.JsonFile.toString()))
                .defaultValue("")
                .build()
        ).define(
            ConfigKeyBuilder.of(GOOGLE_LOGGING_DEFAULT_RESOURCE_TYPE_CONFIG, ConfigDef.Type.STRING)
                .importance(ConfigDef.Importance.HIGH)
                .documentation(GOOGLE_LOGGING_DEFAULT_RESOURCE_TYPE_DOC)
                .importance(ConfigDef.Importance.HIGH)
                .defaultValue("global")
                .build()
        );
  }
}
