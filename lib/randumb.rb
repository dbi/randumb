require 'active_support/core_ext/module/delegation'
require 'active_record/relation'

module Randumb
  
  module ActiveRecord
    
    module Relation
      
      def random(max_items = nil, options={})
        if (!max_items && options[:method] != :select) || options[:method] == :offset
          random_using_offset(max_items)
        else
          random_using_ids(max_items)
        end
      end

      private

      def random_using_ids(max_items = nil)
        # return only the first record if method was called without parameters
        return_first_record = max_items.nil?
        max_items ||= 1

        # take out limit from relation to use later
    
        relation = clone
      
        # store these for including at the end
        original_includes = relation.includes_values
        original_selects = relation.select_values
        
        # clear these for our id only query
        relation.select_values = []
        relation.includes_values = []
      
        # does their original query but only for id fields
        id_only_relation = relation.select("#{table_name}.id")
        id_results = connection.select_all(id_only_relation.to_sql)
      
        ids = {}
      
        while( ids.length < max_items && ids.length < id_results.length )
          rand_index = rand( id_results.length )
          ids[rand_index] = id_results[rand_index]["id"] unless ids.has_key?(rand_index)
        end

        records = klass.select(original_selects).includes(original_includes).find_all_by_id(ids.values)

        return_first_record ? records.first : records
      end

      def random_using_offset(max_results=nil)
        records = []
        taken = Set.new
        if (max = count) > 0
          while (size = records.size) < (max_results || 1) && size < max
            offset = rand(max)
            unless taken.include?(offset)
              taken.add(offset)
              records << find(:first, :offset => offset)
            end
          end
        end
        max_results ? records : records.first
      end

    end # Relation
    
    module Base
      
      # Class method
      def random(max_items=nil, options={})
        relation.random(max_items, options)
      end
      
    end # Base
    
  end # ActiveRecord
  
end # Randumb

# Mix it in
class ActiveRecord::Relation
  include Randumb::ActiveRecord::Relation
end

class ActiveRecord::Base
  extend Randumb::ActiveRecord::Base
end
