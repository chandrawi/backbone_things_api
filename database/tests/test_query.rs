#[cfg(test)]
mod tests {
    use sqlx::types::chrono::DateTime;
    use uuid::Uuid;
    // use bbthings_database::common::utility::{generate_access_key, generate_token_string};
    // use bbthings_database::auth::{api, role, user, profile, token};
    use bbthings_database::auth::{api, role, user, profile, token};
    use bbthings_database::resource::{model, device, group, set, data, buffer, slice};
    use bbthings_database::{DataType, DataValue, SetMember, SetTemplateMember};
    use bbthings_database::common::tag as Tag;

    fn clean_string(s: &str) -> String {
        let mut cleaned = String::new();
        let mut last_char_was_space = false;
        for c in s.chars() {
            if c.is_whitespace() {
                // Treat all whitespace (including newlines) as a potential space
                if !last_char_was_space {
                    cleaned.push(' ');
                    last_char_was_space = true;
                }
            } else {
                cleaned.push(c);
                last_char_was_space = false;
            }
        }
        // Trim leading/trailing spaces if any
        cleaned.trim().to_string()
    }

    #[sqlx::test]
    async fn test_query_auth()
    {
        // API query test
        let id = Uuid::try_parse("b809a61f-8e8a-4555-bcbc-1f5af4327a83").unwrap();
        let ids = [
            Uuid::try_parse("a2b08f8d-db98-4be3-a2d8-3bdf9f2acb6b").unwrap(), 
            Uuid::try_parse("6954c175-5923-4ade-abef-9f03f9e17ee0").unwrap()
        ];
        let name = "Resource testing";
        let address = "localhost:9002";
        let category = "MAIN";
        let password_hash = "$argon2id$v=19$m=19456,t=2,p=1$ya82PGulgVf8pfn12OHjGA$B4yS2r8CnNUvzYhlArOCOZosi8pcSYPn3RPw/LpXqnY";
        let access_key = &[10, 34, 43, 120, 89, 234, 212, 134, 91, 4, 78, 92, 167, 220, 12, 9, 0, 18, 254, 45, 67, 189, 33, 43, 154, 234, 125, 90, 1, 32, 198, 157];
        let qs = api::select_api(None, Some(&ids), None, Some(name), None);
        let s = r#"
            SELECT "api"."api_id", "api"."name", "api"."address", "api"."category", "api"."description", "api"."password", "api"."access_key", 
                "api_procedure"."procedure_id", "api_procedure"."name", "api_procedure"."description", "role"."name" 
            FROM "api" 
            LEFT JOIN "api_procedure" ON "api"."api_id" = "api_procedure"."api_id" 
            LEFT JOIN "role_access" ON "api_procedure"."procedure_id" = "role_access"."procedure_id" 
            LEFT JOIN "role" ON "role_access"."role_id" = "role"."role_id" 
            WHERE "api"."api_id" IN ('a2b08f8d-db98-4be3-a2d8-3bdf9f2acb6b', '6954c175-5923-4ade-abef-9f03f9e17ee0') 
            ORDER BY "api"."api_id" ASC, "api_procedure"."procedure_id" ASC
            "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = api::insert_api(id, name, address, category, "", &password_hash, access_key);
        let s = r#"
            INSERT INTO "api" ("api_id", "name", "address", "category", "description", "password", "access_key")
            VALUES ('b809a61f-8e8a-4555-bcbc-1f5af4327a83', 'Resource testing', 'localhost:9002', 'MAIN', '', 
                '$argon2id$v=19$m=19456,t=2,p=1$ya82PGulgVf8pfn12OHjGA$B4yS2r8CnNUvzYhlArOCOZosi8pcSYPn3RPw/LpXqnY', 
                '\x0A222B7859EAD4865B044E5CA7DC0C090012FE2D43BD212B9AEA7D5A0120C69D')
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = api::update_api(id, None, Some(address), None, None, None, Some(access_key));
        let s = r#"
            UPDATE "api" 
            SET "address" = 'localhost:9002', 
                "access_key" = '\x0A222B7859EAD4865B044E5CA7DC0C090012FE2D43BD212B9AEA7D5A0120C69D' 
            WHERE "api_id" = 'b809a61f-8e8a-4555-bcbc-1f5af4327a83'
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = api::delete_api(id);
        let s = r#"
            DELETE FROM "api" 
            WHERE "api_id" = 'b809a61f-8e8a-4555-bcbc-1f5af4327a83'
        "#;
        assert_eq!(qs.to_string(), clean_string(s));

        // API procedure query test
        let id = Uuid::try_parse("7dac0750-1a8b-49a3-b0fe-9f4fcd3986c6").unwrap();
        let api_id = Uuid::try_parse("27a7dd86-431d-44d0-8944-ce7e87f3e877").unwrap();
        let name = "read_data";
        let qs = api::select_procedure(None, None, Some(api_id), None, Some(name));
        let s = r#"
            SELECT "api_procedure"."procedure_id", "api_procedure"."api_id", "api_procedure"."name", "api_procedure"."description", "role"."name" 
            FROM "api_procedure" 
            LEFT JOIN "role_access" ON "api_procedure"."procedure_id" = "role_access"."procedure_id" 
            LEFT JOIN "role" ON "role_access"."role_id" = "role"."role_id" 
            WHERE "api_procedure"."api_id" = '27a7dd86-431d-44d0-8944-ce7e87f3e877' 
                AND "api_procedure"."name" LIKE '%read_data%' 
            ORDER BY "procedure_id" ASC
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = api::insert_procedure(id, api_id, name, "");
        let s = r#"
            INSERT INTO "api_procedure" ("procedure_id", "api_id", "name", "description") 
            VALUES ('7dac0750-1a8b-49a3-b0fe-9f4fcd3986c6', '27a7dd86-431d-44d0-8944-ce7e87f3e877', 'read_data', '')
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = api::update_procedure(id, Some(name), None);
        let s = r#"
            UPDATE "api_procedure" 
            SET "name" = 'read_data' 
            WHERE "procedure_id" = '7dac0750-1a8b-49a3-b0fe-9f4fcd3986c6'
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = api::delete_procedure(id);
        let s = r#"
            DELETE FROM "api_procedure" 
            WHERE "procedure_id" = '7dac0750-1a8b-49a3-b0fe-9f4fcd3986c6'
        "#;
        assert_eq!(qs.to_string(), clean_string(s));

        // Role query test
        let id = Uuid::try_parse("642433e0-3e5b-4664-8154-efc5b96b9e99").unwrap();
        let ids = [
            Uuid::try_parse("8330170f-2cff-42e3-8ffb-71fbe25b5cd1").unwrap(), 
            Uuid::try_parse("305789d8-6c7b-49de-a648-89d803993bda").unwrap()
        ];
        let api_id = Uuid::try_parse("54f89c0b-381b-4ee0-b068-fa9ab29beec1").unwrap();
        let name = "admin";
        let access_duration = 300;
        let refesh_duration = 86400;
        let procedure_id = Uuid::try_parse("c2df930a-1881-4a7f-8b16-79c52071df43").unwrap();
        let qs = role::select_role(None, Some(&ids), Some(api_id), None, None, Some(name));
        let s = r#"
            SELECT "role"."role_id", "role"."api_id", "role"."name", "role"."multi", "role"."ip_lock", "role"."access_duration", "role"."refresh_duration", "api"."access_key", "role_access"."procedure_id" 
            FROM "role" 
            INNER JOIN "api" ON "role"."api_id" = "api"."api_id" 
            LEFT JOIN "role_access" ON "role"."role_id" = "role_access"."role_id" 
            LEFT JOIN "user_role" ON "role"."role_id" = "user_role"."role_id" 
            WHERE "role"."role_id" IN ('8330170f-2cff-42e3-8ffb-71fbe25b5cd1', '305789d8-6c7b-49de-a648-89d803993bda') 
            ORDER BY "role"."role_id" ASC, "role_access"."procedure_id" ASC
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = role::insert_role(id, api_id, name, false, false, access_duration, refesh_duration);
        let s = r#"
            INSERT INTO "role" ("role_id", "api_id", "name", "multi", "ip_lock", "access_duration", "refresh_duration") 
            VALUES ('642433e0-3e5b-4664-8154-efc5b96b9e99', '54f89c0b-381b-4ee0-b068-fa9ab29beec1', 'admin', FALSE, FALSE, 300, 86400)
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = role::update_role(id, Some(name), Some(true), None, None, Some(refesh_duration));
        let s = r#"
            UPDATE "role" SET "name" = 'admin', "multi" = TRUE, "refresh_duration" = 86400 
            WHERE "role_id" = '642433e0-3e5b-4664-8154-efc5b96b9e99'
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = role::delete_role(id);
        let s = r#"
            DELETE FROM "role" 
            WHERE "role_id" = '642433e0-3e5b-4664-8154-efc5b96b9e99'
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = role::insert_role_access(id, procedure_id);
        let s = r#"
            INSERT INTO "role_access" ("role_id", "procedure_id") 
            VALUES ('642433e0-3e5b-4664-8154-efc5b96b9e99', 'c2df930a-1881-4a7f-8b16-79c52071df43')
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = role::delete_role_access(id, procedure_id);
        let s = r#"
            DELETE FROM "role_access" 
            WHERE "role_id" = '642433e0-3e5b-4664-8154-efc5b96b9e99' AND "procedure_id" = 'c2df930a-1881-4a7f-8b16-79c52071df43'
        "#;
        assert_eq!(qs.to_string(), clean_string(s));

        // User query test
        let id = Uuid::try_parse("18662027-a613-47ba-ad05-a03d99d2ba4a").unwrap();
        let ids = [
            Uuid::try_parse("cf064179-26a4-42da-8f24-9d71b773b210").unwrap(), 
            Uuid::try_parse("a1af0d46-07fe-41b8-8ed6-2e346afcdfb7").unwrap()
        ];
        let api_id = Uuid::try_parse("4755716d-d1ab-4184-8797-8ee83b3dbc62").unwrap();
        let name = "john";
        let email = "john@mail.com";
        let password_hash = "$argon2id$v=19$m=16,t=2,p=1$OUpTeDdkOFdKVXpKOFdRWQ$R2dRNK6YUppApCMETBLxOA";
        let role_id = Uuid::try_parse("6d32d4be-37ad-489f-ab3a-44a879e8a094").unwrap();
        let qs = user::select_user(None, Some(&ids), Some(api_id), None, None, Some(name));
        let s = r#"
            SELECT "user"."user_id", "user"."name", "user"."password", "user"."email", "user"."phone", "role"."api_id", "role"."name", "role"."multi", "role"."ip_lock", "role"."access_duration", "role"."refresh_duration", "api"."access_key" 
            FROM "user" 
            LEFT JOIN "user_role" ON "user"."user_id" = "user_role"."user_id" 
            LEFT JOIN "role" ON "user_role"."role_id" = "role"."role_id" 
            LEFT JOIN "api" ON "role"."api_id" = "api"."api_id" 
            WHERE "user"."user_id" IN ('cf064179-26a4-42da-8f24-9d71b773b210', 'a1af0d46-07fe-41b8-8ed6-2e346afcdfb7') 
            ORDER BY "user"."user_id" ASC, "user_role"."role_id" ASC
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = user::insert_user(id, name, email, "", password_hash);
        let s = r#"
            INSERT INTO "user" ("user_id", "name", "password", "email", "phone") 
            VALUES ('18662027-a613-47ba-ad05-a03d99d2ba4a', 'john', 
                '$argon2id$v=19$m=16,t=2,p=1$OUpTeDdkOFdKVXpKOFdRWQ$R2dRNK6YUppApCMETBLxOA', 'john@mail.com', '')
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = user::update_user(id, Some(name), Some(email), None, None);
        let s = r#"
            UPDATE "user" 
            SET "name" = 'john', "email" = 'john@mail.com' 
            WHERE "user_id" = '18662027-a613-47ba-ad05-a03d99d2ba4a'
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = user::delete_user(id);
        let s = r#"
            DELETE FROM "user" 
            WHERE "user_id" = '18662027-a613-47ba-ad05-a03d99d2ba4a'
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = user::insert_user_role(id, role_id);
        let s = r#"
            INSERT INTO "user_role" ("user_id", "role_id") 
            VALUES ('18662027-a613-47ba-ad05-a03d99d2ba4a', '6d32d4be-37ad-489f-ab3a-44a879e8a094')
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = user::delete_user_role(id, role_id);
        let s = r#"
            DELETE FROM "user_role" 
            WHERE "user_id" = '18662027-a613-47ba-ad05-a03d99d2ba4a' AND "role_id" = '6d32d4be-37ad-489f-ab3a-44a879e8a094'
        "#;
        assert_eq!(qs.to_string(), clean_string(s));

        // Profile role query test
        let id = 1;
        let role_id = Uuid::try_parse("98d04810-09c3-4b50-a1e6-c58ed00e4470").unwrap();
        let name = "phone";
        let value_type = DataType::StringT;
        let value_default = DataValue::String(String::new());
        let qs = profile::select_role_profile(Some(id), Some(role_id));
        let s = r#"
            SELECT "profile_role"."id", "profile_role"."role_id", "profile_role"."name", "profile_role"."category", "profile_role"."type", "profile_role"."value" 
            FROM "profile_role" 
            WHERE "profile_role"."id" = 1
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = profile::insert_role_profile(role_id, name, value_type, value_default.clone(), "CREDENTIAL");
        let s = r#"
            INSERT INTO "profile_role" ("role_id", "name", "type", "value", "category")
            VALUES ('98d04810-09c3-4b50-a1e6-c58ed00e4470', 'phone', 17, '\x', 'CREDENTIAL')
            RETURNING "id"
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = profile::update_role_profile(id, Some(name), None, Some(value_default), None);
        let s = r#"
            UPDATE "profile_role" 
            SET "name" = 'phone', "type" = '\x' 
            WHERE "id" = 1
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = profile::delete_role_profile(id);
        let s = r#"
            DELETE FROM "profile_role" 
            WHERE "id" = 1
        "#;
        assert_eq!(qs.to_string(), clean_string(s));

        // Profile user query test
        let id = 1;
        let user_id = Uuid::try_parse("f4578939-a37e-4825-be8b-bd07516cc4e4").unwrap();
        let name = "phone";
        let value = DataValue::String(String::from("+6281234567890"));
        let qs = profile::select_user_profile(Some(id), Some(user_id));
        let s = r#"
            SELECT "profile_user"."id", "profile_user"."user_id", "profile_user"."name", "profile_user"."category", "profile_user"."type", "profile_user"."value" 
            FROM "profile_user" 
            WHERE "profile_user"."id" = 1
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = profile::insert_user_profile(user_id, name, value.clone(), "CREDENTIAL");
        let s = r#"
            INSERT INTO "profile_user" ("user_id", "name", "value", "type", "category") 
            VALUES ('f4578939-a37e-4825-be8b-bd07516cc4e4', 'phone', '\x2B36323831323334353637383930', 17, 'CREDENTIAL')
            RETURNING "id"
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = profile::update_user_profile(id, Some(name), Some(value.clone()), None);
        let s = r#"
            UPDATE "profile_user" 
            SET "name" = 'phone', "value" = '\x2B36323831323334353637383930', "type" = 17 
            WHERE "id" = 1
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = profile::delete_user_profile(id);
        let s = r#"
            DELETE FROM "profile_user" 
            WHERE "id" = 1
        "#;
        assert_eq!(qs.to_string(), clean_string(s));

        // Token query test
        let user_id = Uuid::try_parse("159192e5-1c39-4032-abb3-8b302d17f99b").unwrap();
        let begin = DateTime::parse_from_str("2023-05-07 07:08:48.123456 +0000", "%Y-%m-%d %H:%M:%S.%6f %z").unwrap().into();
        let end = DateTime::parse_from_str("2025-06-11 14:49:36.123456 +0000", "%Y-%m-%d %H:%M:%S.%6f %z").unwrap().into();
        let selector = token::TokenSelector::ExpiredRange(begin, end, Some(user_id));
        let refresh_token = ["KG7LdKAnVPLZmGkq0TcBzfDJI-UWxhrJ", "sZQZKJgONyaKz698Lmte8viKiVexbsmR"];
        let auth_token = "4AIoTejs1cfYKnuVTMG18C1JMQf9NT_Y";
        let qs = token::select_token(selector);
        let s = r#"
            SELECT "access_id", "user_id", "refresh_token", "auth_token", "created", "expired", "ip" 
            FROM "token" 
            WHERE "expired" >= '2023-05-07 07:08:48.123456 +00:00' AND "expired" <= '2025-06-11 14:49:36.123456 +00:00' 
                AND "user_id" = '159192e5-1c39-4032-abb3-8b302d17f99b' 
            ORDER BY "access_id" ASC
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = token::insert_token(user_id, &refresh_token, auth_token, begin, &[127, 0, 0, 1]);
        let s = r#"
            INSERT INTO "token" ("user_id", "refresh_token", "auth_token", "expired", "ip") 
            VALUES 
                ('159192e5-1c39-4032-abb3-8b302d17f99b', 'KG7LdKAnVPLZmGkq0TcBzfDJI-UWxhrJ', '4AIoTejs1cfYKnuVTMG18C1JMQf9NT_Y', '2023-05-07 07:08:48.123456 +00:00', '\x7F000001'), 
                ('159192e5-1c39-4032-abb3-8b302d17f99b', 'sZQZKJgONyaKz698Lmte8viKiVexbsmR', '4AIoTejs1cfYKnuVTMG18C1JMQf9NT_Y', '2023-05-07 07:08:48.123456 +00:00', '\x7F000001') 
            RETURNING "access_id"
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let selector = token::TokenSelector::Access(1);
        let qs = token::update_token(selector, None, Some(end), None);
        let s = r#"
            UPDATE "token" 
            SET "expired" = '2025-06-11 14:49:36.123456 +00:00' 
            WHERE "token"."access_id" = 1
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let selector = token::TokenSelector::Auth(String::from(auth_token));
        let qs = token::delete_token(selector);
        let s = r#"
            DELETE FROM "token" 
            WHERE "token"."auth_token" = '4AIoTejs1cfYKnuVTMG18C1JMQf9NT_Y'
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
    }

    #[sqlx::test]
    async fn test_query_resource()
    {
        // Model query test
        let model_id = Uuid::try_parse("d9239c8b-e008-48dd-bc58-18334cdda697").unwrap();
        let model_ids = [
            Uuid::try_parse("cd2c6b2f-baa7-4e75-87af-4bc99e0138df").unwrap(),
            Uuid::try_parse("92742292-4078-4c70-b6be-3e8e4907cb17").unwrap()
        ];
        let name = "raw data 1";
        let category = "raw";
        let data_type = [DataType::U8T, DataType::U16T];
        let qs = model::select_model(None, Some(&model_ids), None, Some(name), Some(category));
        let s = r#"
            SELECT "model"."model_id", "model"."name", "model"."category", "model"."description", "model"."data_type", "model_tag"."tag", "model_config"."id", "model_config"."index", "model_config"."name", "model_config"."category", "model_config"."type", "model_config"."value" 
            FROM "model" 
            LEFT JOIN "model_tag" ON "model"."model_id" = "model_tag"."model_id" 
            LEFT JOIN "model_config" ON "model"."model_id" = "model_config"."model_id" 
            WHERE "model"."model_id" IN ('cd2c6b2f-baa7-4e75-87af-4bc99e0138df', '92742292-4078-4c70-b6be-3e8e4907cb17') 
            ORDER BY "model"."model_id" ASC, "model_tag"."tag" ASC, "model_config"."id" ASC
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = model::insert_model(model_id, name, category, "", &data_type);
        let s = r#"
            INSERT INTO "model" ("model_id", "name", "category", "description", "data_type") 
            VALUES ('d9239c8b-e008-48dd-bc58-18334cdda697', 'raw data 1', 'raw', '', '\x0607')
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = model::update_model(model_id, Some(name), Some(category), None, Some(&data_type));
        let s = r#"
            UPDATE "model" 
            SET "category" = 'raw', "name" = 'raw data 1', "data_type" = '\x0607' 
            WHERE "model_id" = 'd9239c8b-e008-48dd-bc58-18334cdda697'
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = model::delete_model(model_id);
        let s = r#"
            DELETE FROM "model" 
            WHERE "model_id" = 'd9239c8b-e008-48dd-bc58-18334cdda697'
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let config_id = 1;
        let name = "unit";
        let value = DataValue::String(String::from("unitless"));
        let category = "unit";
        let qs = model::select_model_config(Some(config_id), Some(model_id));
        let s = r#"
            SELECT "id", "model_id", "index", "name", "category", "type", "value" 
            FROM "model_config" 
            WHERE "id" = 1 
            ORDER BY "model_id" ASC, "index" ASC, "id" ASC
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = model::insert_model_config(model_id, 0, name, value.clone(), category);
        let s = r#"
            INSERT INTO "model_config" ("model_id", "index", "name", "value", "type", "category") 
            VALUES ('d9239c8b-e008-48dd-bc58-18334cdda697', 0, 'unit', '\x756E69746C657373', 17, 'unit') 
            RETURNING "id"
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = model::update_model_config(config_id, Some(name), Some(value), None);
        let s = r#"
            UPDATE "model_config" 
            SET "name" = 'unit', "value" = '\x756E69746C657373', "type" = 17 
            WHERE "id" = 1
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = model::delete_model_config(config_id);
        let s = r#"
            DELETE FROM "model_config" 
            WHERE "id" = 1
        "#;
        assert_eq!(qs.to_string(), clean_string(s));

        // Tag query test
        let tag = Tag::DEFAULT;
        let set_id = Uuid::try_parse("185a67ef-c49c-47fe-bdc1-57671a023657").unwrap();
        let name = "default";
        let members = [-1, 0, 1];
        let qs = model::select_model_tag(model_id, Some(tag));
        let s = r#"
            SELECT "model_tag"."model_id", "model_tag"."tag", "model_tag"."name", "model_tag_member"."member" 
            FROM "model_tag" INNER JOIN "model_tag_member" ON "model_tag"."model_id" = "model_tag_member"."model_id" AND "model_tag"."tag" = "model_tag_member"."tag" WHERE "model_id" = 'd9239c8b-e008-48dd-bc58-18334cdda697' AND "tag" = 0 
            ORDER BY "tag" ASC
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = model::select_tag_members(&[], tag);
        let s = r#"
            SELECT 0 AS "member" 
            FROM "model_tag_member" 
            LIMIT 1
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = model::select_tag_members_set(set_id, tag);
        let s = r#"
            SELECT "member" 
            FROM "model_tag_member" 
            INNER JOIN "set_member" ON "model_tag_member"."model_id" = "set_member"."model_id" 
            WHERE "set_id" = '185a67ef-c49c-47fe-bdc1-57671a023657' AND "tag" = 0
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = model::insert_model_tag(model_id, tag, name);
        let s = r#"
            INSERT INTO "model_tag" ("model_id", "tag", "name") 
            VALUES ('d9239c8b-e008-48dd-bc58-18334cdda697', 0, 'default')
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = model::update_model_tag(model_id, tag, Some(name));
        let s = r#"
            UPDATE "model_tag" 
            SET "name" = 'default' 
            WHERE "model_id" = 'd9239c8b-e008-48dd-bc58-18334cdda697' AND "tag" = 0
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = model::delete_model_tag(model_id, tag);
        let s = r#"
            DELETE FROM "model_tag" 
            WHERE "model_id" = 'd9239c8b-e008-48dd-bc58-18334cdda697' AND "tag" = 0
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = model::insert_model_tag_members(model_id, tag, &members);
        let s = r#"
            INSERT INTO "model_tag_member" ("model_id", "tag", "member") 
            VALUES 
                ('d9239c8b-e008-48dd-bc58-18334cdda697', 0, -1), 
                ('d9239c8b-e008-48dd-bc58-18334cdda697', 0, 0), 
                ('d9239c8b-e008-48dd-bc58-18334cdda697', 0, 1)
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = model::delete_model_tag_members(model_id, tag);
        let s = r#"
            DELETE FROM "model_tag_member" 
            WHERE "model_id" = 'd9239c8b-e008-48dd-bc58-18334cdda697' AND "tag" = 0
        "#;
        assert_eq!(qs.to_string(), clean_string(s));

        // Device query test
        let device_id = Uuid::try_parse("99402107-6a0f-4008-9a4a-4f8ea0488c5d").unwrap();
        let gateway_id = Uuid::try_parse("51eb0548-9d15-4843-aaf7-63f379aa133a").unwrap();
        let type_id = Uuid::try_parse("ea9dc65c-8b92-4489-a855-4fd27407fb38").unwrap();
        let serial_number = "DEVICE01";
        let qs = device::select_device(device::DeviceKind::Gateway, None, None, None, None, Some(type_id), Some(name));
        let s = r#"
            SELECT "device"."device_id", "device"."gateway_id", "device"."type_id", "device"."serial_number", "device"."name", "device"."description", 
                "device_type"."name", "device_type_model"."model_id", "device_config"."id", "device_config"."name", "device_config"."category", "device_config"."type", "device_config"."value" 
            FROM "device" 
            INNER JOIN "device_type" ON "device"."type_id" = "device_type"."type_id" 
            LEFT JOIN "device_type_model" ON "device"."type_id" = "device_type_model"."type_id" 
            LEFT JOIN "device_config" ON "device"."device_id" = "device_config"."device_id" 
            WHERE "device"."type_id" = 'ea9dc65c-8b92-4489-a855-4fd27407fb38' 
            AND "device"."name" LIKE '%default%' 
            AND "device"."device_id" = "device"."gateway_id" 
            ORDER BY "device"."device_id" ASC, "device_type_model"."model_id" ASC, "device_config"."id" ASC
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = device::insert_device(device_id, gateway_id, type_id, serial_number, name, "");
        let s = r#"
            INSERT INTO "device" ("device_id", "gateway_id", "type_id", "serial_number", "name", "description") 
            VALUES ('99402107-6a0f-4008-9a4a-4f8ea0488c5d', '51eb0548-9d15-4843-aaf7-63f379aa133a', 'ea9dc65c-8b92-4489-a855-4fd27407fb38', 'DEVICE01', 'default', '')
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = device::update_device(device::DeviceKind::Device, device_id, Some(gateway_id), None, None, Some(name), None);
        let s = r#"
            UPDATE "device" 
            SET "gateway_id" = '51eb0548-9d15-4843-aaf7-63f379aa133a', "name" = 'default' 
            WHERE "device_id" = '99402107-6a0f-4008-9a4a-4f8ea0488c5d'
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = device::delete_device(device::DeviceKind::Device, device_id);
        let s = r#"
            DELETE FROM "device" 
            WHERE "device_id" = '99402107-6a0f-4008-9a4a-4f8ea0488c5d'
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let config_id = 1;
        let name = "offset";
        let value = DataValue::I32(100);
        let category = "analysis";
        let qs = device::select_device_config(device::DeviceKind::Device, None, Some(device_id));
        let s = r#"
            SELECT "device_config"."id", "device_config"."device_id", "device_config"."name", "device_config"."category", "device_config"."type", "device_config"."value", "device"."gateway_id" 
            FROM "device_config" 
            INNER JOIN "device" ON "device_config"."device_id" = "device"."device_id" 
            WHERE "device_config"."device_id" = '99402107-6a0f-4008-9a4a-4f8ea0488c5d' 
            ORDER BY "device_config"."device_id" ASC, "device_config"."id" ASC
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = device::insert_device_config(device_id, name, value.clone(), category);
        let s = r#"
            INSERT INTO "device_config" ("device_id", "name", "value", "type", "category") 
            VALUES ('99402107-6a0f-4008-9a4a-4f8ea0488c5d', 'offset', '\x00000064', 3, 'analysis') 
            RETURNING "id"
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = device::update_device_config(config_id, Some(name), Some(value), None);
        let s = r#"
            UPDATE "device_config" 
            SET "name" = 'offset', "value" = '\x00000064', "type" = 3 
            WHERE "id" = 1
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = device::delete_device_config(config_id);
        let s = r#"
            DELETE FROM "device_config" 
            WHERE "id" = 1
        "#;
        assert_eq!(qs.to_string(), clean_string(s));

        // Type query test
        let name = "sensor device";
        let qs = device::select_device_type(None, None, Some(name));
        let s = r#"
            SELECT "device_type"."type_id", "device_type"."name", "device_type"."description", "device_type_model"."model_id", 
                "device_type_config"."id", "device_type_config"."name", "device_type_config"."category", "device_type_config"."type", "device_type_config"."value" 
            FROM "device_type" 
            LEFT JOIN "device_type_model" ON "device_type"."type_id" = "device_type_model"."type_id" 
            LEFT JOIN "device_type_config" ON "device_type"."type_id" = "device_type_config"."type_id" 
            WHERE "device_type"."name" LIKE '%sensor device%' 
            ORDER BY "device_type"."type_id" ASC, "device_type_model"."model_id" ASC
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = device::insert_device_type(type_id, name, "");
        let s = r#"
            INSERT INTO "device_type" ("type_id", "name", "description") 
            VALUES ('ea9dc65c-8b92-4489-a855-4fd27407fb38', 'sensor device', '')
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = device::update_device_type(type_id, Some(name), None);
        let s = r#"
            UPDATE "device_type" 
            SET "name" = 'sensor device' 
            WHERE "type_id" = 'ea9dc65c-8b92-4489-a855-4fd27407fb38'
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = device::delete_device_type(type_id);
        let s = r#"
            DELETE FROM "device_type" 
            WHERE "type_id" = 'ea9dc65c-8b92-4489-a855-4fd27407fb38'
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = device::insert_device_type_model(type_id, model_id);
        let s = r#"
            INSERT INTO "device_type_model" ("type_id", "model_id") 
            VALUES ('ea9dc65c-8b92-4489-a855-4fd27407fb38', 'd9239c8b-e008-48dd-bc58-18334cdda697')
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = device::delete_device_type_model(type_id, model_id);
        let s = r#"
            DELETE FROM "device_type_model" 
            WHERE "type_id" = 'ea9dc65c-8b92-4489-a855-4fd27407fb38' 
            AND "model_id" = 'd9239c8b-e008-48dd-bc58-18334cdda697'
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let config_id = 1;
        let name = "offset";
        let value_type = DataType::I32T;
        let value_default = DataValue::I32(100);
        let category = "analysis";
        let qs = device::select_device_type_config(None, Some(type_id));
        let s = r#"
            SELECT "id", "type_id", "name", "category", "type", "value" 
            FROM "device_config" 
            WHERE "type_id" = 'ea9dc65c-8b92-4489-a855-4fd27407fb38' 
            ORDER BY "type_id" ASC, "id" ASC
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = device::insert_device_type_config(type_id, name, value_type.clone(), value_default.clone(), category);
        let s = r#"
            INSERT INTO "device_type_config" ("type_id", "name", "type", "value", "category") 
            VALUES ('ea9dc65c-8b92-4489-a855-4fd27407fb38', 'offset', 3, '\x00000064', 'analysis') 
            RETURNING "id"
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = device::update_device_type_config(config_id, Some(name), Some(value_type), Some(value_default), None);
        let s = r#"
            UPDATE "device_type_config" 
            SET "name" = 'offset', "type" = 3, "value" = '\x00000064' 
            WHERE "type_id" = 1
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = device::delete_device_type_config(config_id);
        let s = r#"
            DELETE FROM "device_type_config" 
            WHERE "id" = 1
        "#;
        assert_eq!(qs.to_string(), clean_string(s));

        // Group query test
        let group_id = Uuid::try_parse("a6aef17d-475d-4ddb-a0fe-2ac049553064").unwrap();
        let member_id = Uuid::try_parse("ecf36ffc-f39c-4119-b9d9-0b0006e0bd8b").unwrap();
        let name = "sensor device 1";
        let category = "sensor";
        let qs = group::select_group(group::GroupKind::Model, Some(group_id), None, None, Some(category));
        let s = r#"
            SELECT "group_model"."group_id", "group_model"."name", "group_model"."category", "group_model"."description", "group_model_member"."model_id" 
            FROM "group_model" 
            LEFT JOIN "group_model_member" ON "group_model"."group_id" = "group_model_member"."group_id" 
            WHERE "group_model"."group_id" = 'a6aef17d-475d-4ddb-a0fe-2ac049553064' 
            ORDER BY "group_model"."group_id" ASC, "group_model_member"."model_id" ASC
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = group::insert_group(group::GroupKind::Device, group_id, name, category, "");
        let s = r#"
            INSERT INTO "group_device" ("group_id", "name", "kind", "category", "description") 
            VALUES ('a6aef17d-475d-4ddb-a0fe-2ac049553064', 'sensor device 1', FALSE, 'sensor', '')
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = group::update_group(group::GroupKind::Gateway, group_id, Some(name), Some(category), None);
        let s = r#"
            UPDATE "group_device" 
            SET "name" = 'sensor device 1', "category" = 'sensor' 
            WHERE "group_id" = 'a6aef17d-475d-4ddb-a0fe-2ac049553064'
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = group::delete_group(group::GroupKind::Device, group_id);
        let s = r#"
            DELETE FROM "group_device" 
            WHERE "group_id" = 'a6aef17d-475d-4ddb-a0fe-2ac049553064'
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = group::insert_group_member(group::GroupKind::Model, group_id, member_id);
        let s = r#"
            INSERT INTO "group_model_member" ("group_id", "model_id") 
            VALUES ('a6aef17d-475d-4ddb-a0fe-2ac049553064', 'ecf36ffc-f39c-4119-b9d9-0b0006e0bd8b')
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = group::delete_group_member(group::GroupKind::Device, group_id, member_id);
        let s = r#"
            DELETE FROM "group_device_member" 
            WHERE "group_id" = 'a6aef17d-475d-4ddb-a0fe-2ac049553064' 
            AND "device_id" = 'ecf36ffc-f39c-4119-b9d9-0b0006e0bd8b'
        "#;
        assert_eq!(qs.to_string(), clean_string(s));

        // Set query test
        let set_id = Uuid::try_parse("2126d3bb-ad0e-4c11-b75f-fb161757fcaa").unwrap();
        let template_id = Uuid::try_parse("024d20b1-106e-4990-8927-c8918ab3aa9d").unwrap();
        let set_ids = [
            Uuid::try_parse("42f7b998-2f93-4d94-814e-4978cb47771f").unwrap(),
            Uuid::try_parse("57a0ca29-be83-4ca6-ae43-4d88718bc153").unwrap()
        ];
        let name = "data collection";
        let qs = set::select_set(None, Some(&set_ids), None, Some(name));
        let s = r#"
            SELECT "set"."set_id", "set"."template_id", "set"."name", "set"."description", "set_member"."device_id", "set_member"."model_id", "set_member"."data_index" 
            FROM "set" 
            LEFT JOIN "set_member" ON "set"."set_id" = "set_member"."set_id" 
            WHERE "set"."set_id" IN ('42f7b998-2f93-4d94-814e-4978cb47771f', '57a0ca29-be83-4ca6-ae43-4d88718bc153') 
            ORDER BY "set"."set_id" ASC, "set_member"."set_position" ASC
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = set::insert_set(set_id, template_id, name, "");
        let s = r#"
            INSERT INTO "set" ("set_id", "template_id", "name", "description") 
            VALUES ('2126d3bb-ad0e-4c11-b75f-fb161757fcaa', '024d20b1-106e-4990-8927-c8918ab3aa9d', 'data collection', '')
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = set::update_set(set_id, Some(template_id), Some(name), None);
        let s = r#"
            UPDATE "set" 
            SET "template_id" = '024d20b1-106e-4990-8927-c8918ab3aa9d', "name" = 'data collection' 
            WHERE "set_id" = '2126d3bb-ad0e-4c11-b75f-fb161757fcaa'
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = set::delete_set(set_id);
        let s = r#"
            DELETE FROM "set" 
            WHERE "set_id" = '2126d3bb-ad0e-4c11-b75f-fb161757fcaa'
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let member = SetMember {
            device_id: Uuid::try_parse("7fcc57f3-2888-44f8-b179-77e5ad1cd7a4").unwrap(),
            model_id: Uuid::try_parse("d10769a3-75f7-4670-8982-ccce5618dcdd").unwrap(),
            data_index: vec![0, 1]
        };
        let qs = set::insert_set_members(set_id, &[member]);
        let s = r#"
            INSERT INTO "set_member" ("set_id", "device_id", "model_id", "data_index", "set_position", "set_number") 
            VALUES ('2126d3bb-ad0e-4c11-b75f-fb161757fcaa', '7fcc57f3-2888-44f8-b179-77e5ad1cd7a4', 'd10769a3-75f7-4670-8982-ccce5618dcdd', '\x0001', 0, 2)
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = set::delete_set_members(set_id);
        let s = r#"
            DELETE FROM "set_member" 
            WHERE "set_id" = '2126d3bb-ad0e-4c11-b75f-fb161757fcaa' 
            RETURNING "device_id", "model_id", "data_index", "set_position"
        "#;
        assert_eq!(qs.to_string(), clean_string(s));

        // Set template query test
        let template_id = Uuid::try_parse("6b9952eb-7812-46e4-bbbd-55ea7087ad6c").unwrap();
        let template_ids = [
            Uuid::try_parse("a6612c92-8521-414a-a7cb-2097e55514e1").unwrap(),
            Uuid::try_parse("bb22102d-d3a8-4586-a88f-e88a2f7c1a67").unwrap()
        ];
        let name = "data collection";
        let qs = set::select_set_template(None, Some(&template_ids), Some(name));
        let s = r#"
            SELECT "set_template"."template_id", "set_template"."name", "set_template"."description", "set_template_member"."type_id", "set_template_member"."model_id", "set_template_member"."data_index" 
            FROM "set_template" 
            LEFT JOIN "set_template_member" ON "set_template"."template_id" = "set_template_member"."template_id" 
            WHERE "set_template"."template_id" IN ('a6612c92-8521-414a-a7cb-2097e55514e1', 'bb22102d-d3a8-4586-a88f-e88a2f7c1a67') 
            ORDER BY "set_template"."template_id" ASC, "set_template_member"."template_index" ASC
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = set::insert_set_template(template_id, name, "");
        let s = r#"
            INSERT INTO "set_template" ("template_id", "name", "description") 
            VALUES ('6b9952eb-7812-46e4-bbbd-55ea7087ad6c', 'data collection', '')
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = set::update_set_template(template_id, Some(name), None);
        let s = r#"
            UPDATE "set_template" 
            SET "name" = 'data collection' 
            WHERE "template_id" = '6b9952eb-7812-46e4-bbbd-55ea7087ad6c'
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = set::delete_set_template(template_id);
        let s = r#"
            DELETE FROM "set_template" 
            WHERE "template_id" = '6b9952eb-7812-46e4-bbbd-55ea7087ad6c'
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let member = SetTemplateMember {
            type_id: Uuid::try_parse("432f1a3d-a10a-4cc7-8b3a-a1edd41651bc").unwrap(),
            model_id: Uuid::try_parse("ea811918-dbc0-455a-963f-c7889b0170c5").unwrap(),
            data_index: vec![0, 1]
        };
        let qs = set::insert_set_template_members(template_id, &[member]);
        let s = r#"
            INSERT INTO "set_template_member" ("template_id", "type_id", "model_id", "data_index", "template_index") 
            VALUES ('6b9952eb-7812-46e4-bbbd-55ea7087ad6c', '432f1a3d-a10a-4cc7-8b3a-a1edd41651bc', 'ea811918-dbc0-455a-963f-c7889b0170c5', '\x0001', 0)
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = set::delete_set_members(template_id);
        let s = r#"
            DELETE FROM "set_member" 
            WHERE "set_id" = '6b9952eb-7812-46e4-bbbd-55ea7087ad6c' 
            RETURNING "device_id", "model_id", "data_index", "set_position"
        "#;
        assert_eq!(qs.to_string(), clean_string(s));

        // Data query test
        let device_id = Uuid::try_parse("773fe850-10d0-4012-8c0a-495dc6990b18").unwrap();
        let device_ids = [
            Uuid::try_parse("a2be5346-4014-4844-9a60-56e3392c1ce3").unwrap(),
            Uuid::try_parse("f55c7ded-3615-4ab4-9fa3-c05f71668f68").unwrap()
        ];
        let model_id = Uuid::try_parse("df467d0a-4904-4162-b08b-bd4b992cdefe").unwrap();
        let model_ids = [
            Uuid::try_parse("38723da0-768d-4570-9be7-f8808f7c10c1").unwrap(),
            Uuid::try_parse("183550a1-e55e-421d-9e34-fb7bf834195c").unwrap()
        ];
        let timestamp = DateTime::parse_from_str("2023-05-07 07:08:48.123456 +0000", "%Y-%m-%d %H:%M:%S.%6f %z").unwrap().into();
        let tag = Tag::ERROR;
        let data_value = [DataValue::I32(1000), DataValue::F64(-0.12345), DataValue::String(String::from("abc"))];
        let selector = data::DataSelector::Time(timestamp);
        let qs = data::select_data(selector, &[device_id], &[model_id], Some(tag));
        let s = r#"
            SELECT "data"."device_id", "data"."model_id", "data"."timestamp", "data"."tag", "data"."data", "model"."data_type" 
            FROM "data" 
            INNER JOIN "model" ON "data"."model_id" = "model"."model_id" 
            WHERE "data"."device_id" = '773fe850-10d0-4012-8c0a-495dc6990b18' 
            AND "data"."model_id" = 'df467d0a-4904-4162-b08b-bd4b992cdefe' 
            AND "data"."timestamp" = '2023-05-07 07:08:48.123456 +00:00' 
            AND "data"."tag" IN 
                (SELECT "member" FROM "model_tag_member" WHERE "model_id" IN ('df467d0a-4904-4162-b08b-bd4b992cdefe') AND "tag" = -1)
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let selector = data::DataSelector::Later(timestamp);
        let qs = data::select_data(selector, &device_ids, &model_ids, Some(tag));
        let s = r#"
            SELECT "data"."device_id", "data"."model_id", "data"."timestamp", "data"."tag", "data"."data", "model"."data_type" 
            FROM "data" 
            INNER JOIN "model" ON "data"."model_id" = "model"."model_id" 
            WHERE "data"."device_id" IN ('a2be5346-4014-4844-9a60-56e3392c1ce3', 'f55c7ded-3615-4ab4-9fa3-c05f71668f68') 
            AND "data"."model_id" IN ('38723da0-768d-4570-9be7-f8808f7c10c1', '183550a1-e55e-421d-9e34-fb7bf834195c') 
            AND "data"."timestamp" > '2023-05-07 07:08:48.123456 +00:00' 
            AND "data"."tag" IN 
                (SELECT "member" FROM "model_tag_member" WHERE "model_id" IN ('38723da0-768d-4570-9be7-f8808f7c10c1', '183550a1-e55e-421d-9e34-fb7bf834195c') AND "tag" = -1)
            ORDER BY "data"."timestamp" ASC
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = data::insert_data(device_id, model_id, timestamp, &data_value, Some(tag));
        let s = r#"
            INSERT INTO "data" ("device_id", "model_id", "timestamp", "tag", "data") 
            VALUES ('773fe850-10d0-4012-8c0a-495dc6990b18', 'df467d0a-4904-4162-b08b-bd4b992cdefe', '2023-05-07 07:08:48.123456 +00:00', -1, 
                '\x000003E8BFBF9A6B50B0F27C03616263')
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = data::insert_data_multiple(&device_ids, &model_ids, &[timestamp, timestamp], &[&data_value, &data_value], Some(&[tag, tag]));
        let s = r#"
            INSERT INTO "data" ("device_id", "model_id", "timestamp", "tag", "data") 
            VALUES 
                ('a2be5346-4014-4844-9a60-56e3392c1ce3', '38723da0-768d-4570-9be7-f8808f7c10c1', '2023-05-07 07:08:48.123456 +00:00', -1, 
                '\x000003E8BFBF9A6B50B0F27C03616263'),
                ('f55c7ded-3615-4ab4-9fa3-c05f71668f68', '183550a1-e55e-421d-9e34-fb7bf834195c', '2023-05-07 07:08:48.123456 +00:00', -1, 
                '\x000003E8BFBF9A6B50B0F27C03616263')
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = data::delete_data(device_id, model_id, timestamp, Some(tag));
        let s = r#"
            DELETE FROM "data" 
            WHERE "device_id" = '773fe850-10d0-4012-8c0a-495dc6990b18' AND "model_id" = 'df467d0a-4904-4162-b08b-bd4b992cdefe' 
                AND "timestamp" = '2023-05-07 07:08:48.123456 +00:00' AND "tag" = -1
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = data::select_data_types(&model_ids);
        let s = r#"
            SELECT "data_type" 
            FROM "model" 
            WHERE "model_id" IN ('38723da0-768d-4570-9be7-f8808f7c10c1', '183550a1-e55e-421d-9e34-fb7bf834195c') 
            ORDER BY "model_id" ASC
        "#;
        assert_eq!(qs.to_string(), clean_string(s));

        // Dataset and count data test query
        let set_id = Uuid::try_parse("5a964938-b3d7-4b19-99e3-e33e711c8b8e").unwrap();
        let begin = DateTime::parse_from_str("2023-05-07 07:08:48.123456 +0000", "%Y-%m-%d %H:%M:%S.%6f %z").unwrap().into();
        let end = DateTime::parse_from_str("2025-06-11 14:49:36.123456 +0000", "%Y-%m-%d %H:%M:%S.%6f %z").unwrap().into();
        let selector = data::DataSelector::Range(begin, end);
        let qs = data::select_data_set(selector, set_id, Some(tag));
        let s = r#"
            SELECT "data"."device_id", "data"."model_id", "data"."timestamp", "data"."tag", "data"."data", "model"."data_type", "set_member"."data_index", "set_member"."set_position", "set_member"."set_number" 
            FROM "data" 
            INNER JOIN "model" ON "data"."model_id" = "model"."model_id" 
            INNER JOIN "set_member" ON "data"."device_id" = "set_member"."device_id" AND "data"."model_id" = "set_member"."model_id" 
            WHERE "set_member"."set_id" = '5a964938-b3d7-4b19-99e3-e33e711c8b8e' 
            AND "data"."timestamp" >= '2023-05-07 07:08:48.123456 +00:00' 
            AND "data"."timestamp" <= '2025-06-11 14:49:36.123456 +00:00' 
            AND "data"."tag" IN 
                (SELECT "member" FROM "model_tag_member" INNER JOIN "set_member" ON "model_tag_member"."model_id" = "set_member"."model_id" WHERE "set_id" = '5a964938-b3d7-4b19-99e3-e33e711c8b8e' AND "tag" = -1) 
            ORDER BY "data"."timestamp" ASC, "data"."tag" ASC, "set_member"."set_position" ASC
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let selector = data::DataSelector::Earlier(timestamp);
        let qs = data::select_data_timestamp(selector, &[device_id], &[model_id], Some(tag));
        let s = r#"
            SELECT "timestamp" 
            FROM "data" 
            WHERE "device_id" = '773fe850-10d0-4012-8c0a-495dc6990b18' 
            AND "model_id" = 'df467d0a-4904-4162-b08b-bd4b992cdefe' 
            AND "timestamp" < '2023-05-07 07:08:48.123456 +00:00' 
            AND "tag" IN 
                (SELECT "member" FROM "model_tag_member" WHERE "model_id" IN ('df467d0a-4904-4162-b08b-bd4b992cdefe') AND "tag" = -1) 
            ORDER BY "timestamp" ASC
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let selector = data::DataSelector::Range(begin, end);
        let qs = data::count_data(selector, &[device_id], &[model_id], Some(tag));
        let s = r#"
            SELECT COUNT("timestamp") 
            FROM "data" 
            WHERE "device_id" = '773fe850-10d0-4012-8c0a-495dc6990b18' 
            AND "model_id" = 'df467d0a-4904-4162-b08b-bd4b992cdefe' 
            AND "timestamp" >= '2023-05-07 07:08:48.123456 +00:00' 
            AND "timestamp" <= '2025-06-11 14:49:36.123456 +00:00' 
            AND "tag" IN 
                (SELECT "member" FROM "model_tag_member" WHERE "model_id" IN ('df467d0a-4904-4162-b08b-bd4b992cdefe') AND "tag" = -1)
        "#;
        assert_eq!(qs.to_string(), clean_string(s));

        // Buffer query test
        let buffer_id = 1;
        let buffer_ids = [1, 2, 3];
        let device_id = Uuid::try_parse("e81a6fb3-731d-45b7-9195-8a1c6690f31b").unwrap();
        let device_ids = [
            Uuid::try_parse("30bd8a90-1669-4d56-a67d-b709b2497156").unwrap(),
            Uuid::try_parse("a3e8f20e-acf2-403d-a3cb-22210a2c68b7").unwrap()
        ];
        let model_id = Uuid::try_parse("0dcb2faa-12a0-4a81-a3e2-5c7dc5252c61").unwrap();
        let model_ids = [
            Uuid::try_parse("e3933119-f5e9-47f5-892b-7dde010df420").unwrap(),
            Uuid::try_parse("7811e22a-6c83-4b47-aeb5-758b01bc4701").unwrap()
        ];
        let timestamp = DateTime::parse_from_str("2023-05-07 07:08:48.123456 +0000", "%Y-%m-%d %H:%M:%S.%6f %z").unwrap().into();
        let number = 100;
        let offset = 200;
        let tag = Tag::ERROR;
        let data_value = [DataValue::I32(-1000), DataValue::F64(0.12345), DataValue::String(String::from("_-"))];
        let selector = buffer::BufferSelector::Time(timestamp);
        let qs = buffer::select_buffer(selector, Some(&buffer_ids), None, None, Some(tag));
        let s = r#"
            SELECT "data_buffer"."id", "data_buffer"."device_id", "data_buffer"."model_id", "data_buffer"."timestamp", "data_buffer"."tag", "data_buffer"."data", "model"."data_type" 
            FROM "data_buffer" 
            INNER JOIN "model" ON "data_buffer"."model_id" = "model"."model_id" 
            WHERE "id" IN (1, 2, 3) 
            AND "data_buffer"."timestamp" = '2023-05-07 07:08:48.123456 +00:00'
            AND "data_buffer"."tag" IN (SELECT -1 AS "member" FROM "model_tag_member" LIMIT 1)
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let selector = buffer::BufferSelector::First(number, offset);
        let qs = buffer::select_buffer(selector, None, Some(&device_ids), Some(&model_ids), Some(tag));
        let s = r#"
            SELECT "data_buffer"."id", "data_buffer"."device_id", "data_buffer"."model_id", "data_buffer"."timestamp", "data_buffer"."tag", "data_buffer"."data", "model"."data_type" 
            FROM "data_buffer" 
            INNER JOIN "model" ON "data_buffer"."model_id" = "model"."model_id" 
            WHERE "data_buffer"."device_id" IN ('30bd8a90-1669-4d56-a67d-b709b2497156', 'a3e8f20e-acf2-403d-a3cb-22210a2c68b7') 
            AND "data_buffer"."model_id" IN ('e3933119-f5e9-47f5-892b-7dde010df420', '7811e22a-6c83-4b47-aeb5-758b01bc4701') 
            AND "data_buffer"."tag" IN 
                (SELECT "member" FROM "model_tag_member" WHERE "model_id" IN ('e3933119-f5e9-47f5-892b-7dde010df420', '7811e22a-6c83-4b47-aeb5-758b01bc4701') AND "tag" = -1) 
            ORDER BY "data_buffer"."id" ASC 
            LIMIT 100 OFFSET 200
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = buffer::insert_buffer(device_id, model_id, timestamp, &data_value, Some(tag));
        let s = r#"
            INSERT INTO "data_buffer" ("device_id", "model_id", "timestamp", "tag", "data") 
            VALUES ('e81a6fb3-731d-45b7-9195-8a1c6690f31b', '0dcb2faa-12a0-4a81-a3e2-5c7dc5252c61', '2023-05-07 07:08:48.123456 +00:00', -1, '\xFFFFFC183FBF9A6B50B0F27C025F2D') 
            RETURNING "id"
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = buffer::insert_buffer_multiple(&device_ids, &model_ids, &[timestamp, timestamp], &[&data_value, &data_value], Some(&[tag, tag]));
        let s = r#"
            INSERT INTO "data_buffer" ("device_id", "model_id", "timestamp", "tag", "data") 
            VALUES 
                ('30bd8a90-1669-4d56-a67d-b709b2497156', 'e3933119-f5e9-47f5-892b-7dde010df420', '2023-05-07 07:08:48.123456 +00:00', -1, '\xFFFFFC183FBF9A6B50B0F27C025F2D'), 
                ('a3e8f20e-acf2-403d-a3cb-22210a2c68b7', '7811e22a-6c83-4b47-aeb5-758b01bc4701', '2023-05-07 07:08:48.123456 +00:00', -1, '\xFFFFFC183FBF9A6B50B0F27C025F2D') 
            RETURNING "id"
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = buffer::update_buffer(None, Some(device_id), Some(model_id), Some(timestamp), Some(&data_value), Some(tag));
        let s = r#"
            UPDATE "data_buffer" 
            SET "data" = '\xFFFFFC183FBF9A6B50B0F27C025F2D' 
            WHERE "device_id" = 'e81a6fb3-731d-45b7-9195-8a1c6690f31b' 
            AND "model_id" = '0dcb2faa-12a0-4a81-a3e2-5c7dc5252c61' 
            AND "timestamp" = '2023-05-07 07:08:48.123456 +00:00' 
            AND "tag" = -1
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = buffer::delete_buffer(Some(buffer_id), None, None, None, None);
        let s = r#"
            DELETE FROM "data_buffer" 
            WHERE "id" = 1
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = buffer::select_buffer_types(buffer_id);
        let s = r#"
            SELECT "model"."data_type" 
            FROM "data_buffer" 
            INNER JOIN "model" ON "data_buffer"."model_id" = "model"."model_id" 
            WHERE "data_buffer"."id" = 1
        "#;
        assert_eq!(qs.to_string(), clean_string(s));

        // Bufferset and count buffer test query
        let set_id = Uuid::try_parse("edcfd3f2-3652-4621-9669-14993340f0f3").unwrap();
        let begin = DateTime::parse_from_str("2023-05-07 07:08:48.123456 +0000", "%Y-%m-%d %H:%M:%S.%6f %z").unwrap().into();
        let end = DateTime::parse_from_str("2025-06-11 14:49:36.123456 +0000", "%Y-%m-%d %H:%M:%S.%6f %z").unwrap().into();
        let selector = buffer::BufferSelector::Range(begin, end);
        let qs = buffer::select_buffer_set(selector, set_id, Some(tag));
        let s = r#"
            SELECT "data_buffer"."id", "data_buffer"."device_id", "data_buffer"."model_id", "data_buffer"."timestamp", "data_buffer"."tag", "data_buffer"."data", "model"."data_type", "set_member"."data_index", "set_member"."set_position", "set_member"."set_number" 
            FROM "data_buffer" 
            INNER JOIN "model" ON "data_buffer"."model_id" = "model"."model_id" 
            INNER JOIN "set_member" ON "data_buffer"."device_id" = "set_member"."device_id" AND "data_buffer"."model_id" = "set_member"."model_id" 
            WHERE "set_member"."set_id" = 'edcfd3f2-3652-4621-9669-14993340f0f3' 
            AND "data_buffer"."timestamp" >= '2023-05-07 07:08:48.123456 +00:00' 
            AND "data_buffer"."timestamp" <= '2025-06-11 14:49:36.123456 +00:00' 
            AND "data_buffer"."tag" IN 
                (SELECT "member" FROM "model_tag_member" INNER JOIN "set_member" ON "model_tag_member"."model_id" = "set_member"."model_id" WHERE "set_id" = 'edcfd3f2-3652-4621-9669-14993340f0f3' AND "tag" = -1) 
            ORDER BY "data_buffer"."timestamp" ASC, "data_buffer"."tag" ASC, "set_member"."set_position" ASC
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let selector = buffer::BufferSelector::Range(begin, end);
        let qs = buffer::select_buffer_timestamp(selector, Some(&[device_id]), Some(&[model_id]), Some(tag));
        let s = r#"
            SELECT "timestamp" 
            FROM "data_buffer" 
            WHERE "device_id" = 'e81a6fb3-731d-45b7-9195-8a1c6690f31b' 
            AND "model_id" = '0dcb2faa-12a0-4a81-a3e2-5c7dc5252c61' 
            AND "timestamp" >= '2023-05-07 07:08:48.123456 +00:00' 
            AND "timestamp" <= '2025-06-11 14:49:36.123456 +00:00' 
            AND "tag" IN 
                (SELECT "member" FROM "model_tag_member" WHERE "model_id" IN ('0dcb2faa-12a0-4a81-a3e2-5c7dc5252c61') AND "tag" = -1) 
            ORDER BY "timestamp" ASC
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let selector = buffer::BufferSelector::Range(begin, end);
        let qs = buffer::count_buffer(selector, &[device_id], &[model_id], Some(tag));
        let s = r#"
            SELECT COUNT("id") 
            FROM "data_buffer" 
            WHERE "device_id" = 'e81a6fb3-731d-45b7-9195-8a1c6690f31b' 
            AND "model_id" = '0dcb2faa-12a0-4a81-a3e2-5c7dc5252c61' 
            AND "timestamp" >= '2023-05-07 07:08:48.123456 +00:00' 
            AND "timestamp" <= '2025-06-11 14:49:36.123456 +00:00' 
            AND "tag" IN 
                (SELECT "member" FROM "model_tag_member" WHERE "model_id" IN ('0dcb2faa-12a0-4a81-a3e2-5c7dc5252c61') AND "tag" = -1)
        "#;
        assert_eq!(qs.to_string(), clean_string(s));

        // Slice test query
        let slice_id = 1;
        let device_id = Uuid::try_parse("7512da80-606c-4502-894e-e28c1ae31af4").unwrap();
        let device_ids = [
            Uuid::try_parse("b739eedf-572d-4641-a3e6-0acb6063574a").unwrap(),
            Uuid::try_parse("1f1ee268-d040-4d88-8106-2436efb4431c").unwrap()
        ];
        let model_id = Uuid::try_parse("64b3eeac-6486-4f3f-9cc5-981686b3a9a4").unwrap();
        let model_ids = [
            Uuid::try_parse("8fc6afbc-593b-401a-ba6a-c67b91f20952").unwrap(),
            Uuid::try_parse("2df8bc61-23a2-4a03-a6bb-c1210ea5fe49").unwrap()
        ];
        let timestamp = DateTime::parse_from_str("2023-05-07 07:08:48.123456 +0000", "%Y-%m-%d %H:%M:%S.%6f %z").unwrap().into();
        let begin = DateTime::parse_from_str("2023-05-07 07:08:48.123456 +0000", "%Y-%m-%d %H:%M:%S.%6f %z").unwrap().into();
        let end = DateTime::parse_from_str("2025-06-11 14:49:36.123456 +0000", "%Y-%m-%d %H:%M:%S.%6f %z").unwrap().into();
        let name = "calibration";
        let selector = slice::SliceSelector::Time(timestamp);
        let qs = slice::select_slice(selector, None, Some(&device_ids), Some(&model_ids), Some(name));
        let s = r#"
            SELECT "id", "device_id", "model_id", "timestamp_begin", "timestamp_end", "name", "description" 
            FROM "slice_data" 
            WHERE "device_id" IN ('b739eedf-572d-4641-a3e6-0acb6063574a', '1f1ee268-d040-4d88-8106-2436efb4431c') 
            AND "model_id" IN ('8fc6afbc-593b-401a-ba6a-c67b91f20952', '2df8bc61-23a2-4a03-a6bb-c1210ea5fe49') 
            AND "name" LIKE '%calibration%' 
            AND "timestamp_begin" <= '2023-05-07 07:08:48.123456 +00:00' 
            AND "timestamp_end" >= '2023-05-07 07:08:48.123456 +00:00' 
            ORDER BY "id" ASC
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = slice::insert_slice(device_id, model_id, begin, end, name, "");
        let s = r#"
            INSERT INTO "slice_data" ("device_id", "model_id", "timestamp_begin", "timestamp_end", "name", "description") 
            VALUES ('7512da80-606c-4502-894e-e28c1ae31af4', '64b3eeac-6486-4f3f-9cc5-981686b3a9a4', '2023-05-07 07:08:48.123456 +00:00', '2025-06-11 14:49:36.123456 +00:00', 'calibration', '') 
            RETURNING "id"
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = slice::update_slice(slice_id, Some(begin), None, Some(name), None);
        let s = r#"
            UPDATE "slice_data" 
            SET "timestamp_begin" = '2023-05-07 07:08:48.123456 +00:00', "name" = 'calibration' 
            WHERE "id" = 1
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = slice::delete_slice(slice_id);
        let s = r#"
            DELETE FROM "slice_data" 
            WHERE "id" = 1
        "#;
        assert_eq!(qs.to_string(), clean_string(s));

        // Slice set test query
        let slice_id = 1;
        let set_id = Uuid::try_parse("247ad544-9d6c-4ee8-9468-6f41c2419cc0").unwrap();
        let timestamp = DateTime::parse_from_str("2023-05-07 07:08:48.123456 +0000", "%Y-%m-%d %H:%M:%S.%6f %z").unwrap().into();
        let begin = DateTime::parse_from_str("2023-05-07 07:08:48.123456 +0000", "%Y-%m-%d %H:%M:%S.%6f %z").unwrap().into();
        let end = DateTime::parse_from_str("2025-06-11 14:49:36.123456 +0000", "%Y-%m-%d %H:%M:%S.%6f %z").unwrap().into();
        let name = "calibration";
        let selector = slice::SliceSelector::Time(timestamp);
        let qs = slice::select_slice_set(selector, None, Some(set_id), Some(name));
        let s = r#"
            SELECT "id", "set_id", "timestamp_begin", "timestamp_end", "name", "description" 
            FROM "slice_data_set" 
            WHERE "set_id" = '247ad544-9d6c-4ee8-9468-6f41c2419cc0' 
            AND "name" LIKE '%calibration%' 
            AND "timestamp_begin" <= '2023-05-07 07:08:48.123456 +00:00' 
            AND "timestamp_end" >= '2023-05-07 07:08:48.123456 +00:00' 
            ORDER BY "id" ASC
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = slice::insert_slice_set(set_id, begin, end, name, "");
        let s = r#"
            INSERT INTO "slice_data_set" ("set_id", "timestamp_begin", "timestamp_end", "name", "description") 
            VALUES ('247ad544-9d6c-4ee8-9468-6f41c2419cc0', '2023-05-07 07:08:48.123456 +00:00', '2025-06-11 14:49:36.123456 +00:00', 'calibration', '') 
            RETURNING "id"
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = slice::update_slice_set(slice_id, Some(begin), None, Some(name), None);
        let s = r#"
            UPDATE "slice_data_set" 
            SET "timestamp_begin" = '2023-05-07 07:08:48.123456 +00:00', "name" = 'calibration' 
            WHERE "id" = 1
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
        let qs = slice::delete_slice_set(slice_id);
        let s = r#"
            DELETE FROM "slice_data_set" 
            WHERE "id" = 1
        "#;
        assert_eq!(qs.to_string(), clean_string(s));
    }

}
